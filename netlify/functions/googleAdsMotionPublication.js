// Explicit, durable publication of reviewed videos to a paused, product-only PMax group.
// Upload receipts, YouTube processing, asset attachment and ad policy are distinct states.
const crypto = require('crypto');
const digest = value => crypto.createHash('sha256').update(JSON.stringify(value)).digest('hex');
const clone = value => JSON.parse(JSON.stringify(value));
const rubric=require('./googleAdsAdQuality');
const KEYS = ['mobile_portrait', 'mobile_square', 'desktop_landscape'];
function selection(job) {
  if (job.phase !== 'ready' || job.quality?.pass !== true || job.quality?.productFaithful !== true) throw Error('The animation must pass its jewelry quality review before Google publication.');
  const variants = KEYS.map(key => job.variants.find(v => v.key === key));
  if (variants.some(v => !v?.asset?.hash || !v.asset.path || v.seconds !== 10)) throw Error('Three complete reviewed video formats are required.');
  return variants;
}
function reviewHash(job) {
  return digest({jobId:job.id, productId:job.productId, groupRef:job.groupRef, destination:job.destination, ...(job.pipelineVersion>=2?{quality:job.quality,copy:job.plan?.copy,nativeCopy:job.plan?.nativeCopy}:{}), variants:selection(job).map(v => ({key:v.key, asset:v.asset}))});
}
function nativeCompatibility(job,rows){
  const copy=job.plan?.nativeCopy||{},types={headlines:'HEADLINE',longHeadlines:'LONG_HEADLINE',descriptions:'DESCRIPTION'};
  const copyMatches=Object.entries(types).every(([key,type])=>Array.isArray(copy[key])&&copy[key].length>0&&JSON.stringify([...new Set(copy[key])].sort())===JSON.stringify([...new Set(rows.filter(r=>r.assetGroupAsset?.fieldType===type).map(r=>r.asset?.textAsset?.text).filter(Boolean))].sort()));
  return {copyMatches,shopNowLinked:rows.some(r=>r.assetGroupAsset?.fieldType==='CALL_TO_ACTION_SELECTION'&&r.asset?.callToActionAsset?.callToAction==='SHOP_NOW')};
}
function safePublication(p) {
  if (!p) return null;
  return {phase:p.phase, error:p.error || null, updatedAt:p.updatedAt, attachedAt:p.attachedAt || null,
    videos:(p.videos || []).map(v => ({key:v.key, state:v.state, resourceName:v.resourceName || null, videoId:v.videoId || null})),
    merchant:p.merchant?{phase:p.merchant.phase,error:p.merchant.error||null,reviewHash:p.merchant.reviewHash,links:p.merchant.plan?.videoLinks||[],identity:p.merchant.plan?.identity||null,source:p.merchant.plan?.sourceName||null}:null,
    verification:p.verification || null};
}
function uploadUrl(value) {
  const u = new URL(value);
  if (u.protocol !== 'https:' || u.hostname !== 'googleads.googleapis.com' || u.username || u.password || u.port) throw Error('Google returned an unsupported upload session URL.');
  return u.href;
}
function createPublicationService(D) {
  async function context(input) {
    const c = await D.context(input.workspaceId);
    if (c.w.archivedAt || String(c.w.settings.productId) !== String(input.productId) || c.w.settings.groupRef !== input.groupRef) throw Error('The selected product or group changed.');
    if (!/^motion_[a-f0-9]{40}$/.test(input.jobId || '')) throw Error('Select a saved animation first.');
    const ref = c.ref.collection('motionJobs').doc(input.jobId), row = await ref.get();
    if (!row.exists) throw Error('The saved animation was not found.');
    const job = row.data();
    if (job.resetAt || String(job.productId) !== String(input.productId) || job.groupRef !== input.groupRef) throw Error('The animation belongs to another product or group.');
    return {...c, ref, job};
  }
  async function start(input) {
    const {ref, job} = await context(input), expectedHash = reviewHash(job);
    if (input.reviewHash !== expectedHash) throw Error('Review the current video files before publishing.');
    if(job.publication?.phase!=='attached'&&(!Number.isFinite(job.quality?.score)||job.quality.score<(job.quality.rubric===rubric.RUBRIC?rubric.TARGET:97)||job.quality.score>100||job.quality.mobileReadable!==true))throw Error('The saved animation has not met its complete-ad quality target. Improve the product scene before a new Google upload.');
    if(job.publication?.phase==='attached'||job.publication?.leaseUntil>Date.now()||job.publication?.nextCheckAt>Date.now())return {ok:true,cached:true,queued:false,publication:safePublication(job.publication)};
    await D.assertTarget(job);
    await D.fb().db.runTransaction(async tx => {
      const row = await tx.get(ref), current = row.data();
      if (reviewHash(current) !== expectedHash) throw Error('The reviewed animation changed.');
      const p = current.publication;
      if (p?.phase === 'blocked') throw Error(p.error || 'Reconcile the previous Google request before retrying.');
      if (p?.phase === 'attached' || p?.leaseUntil > Date.now()) return;
      tx.update(ref, {publication:p || {phase:'queued', reviewHash:expectedHash, createdAt:Date.now(), updatedAt:Date.now(), videos:selection(current).map(v => ({key:v.key, hash:v.asset.hash, state:'NOT_STARTED'}))}});
    });
    return {ok:true, queued:job.publication?.phase !== 'attached', workspaceId:input.workspaceId, jobId:job.id, productId:job.productId, groupRef:job.groupRef};
  }
  async function run(input) {
    const {ref, job} = await context(input), owner = crypto.randomUUID(); let p;
    await D.fb().db.runTransaction(async tx => {
      const row = await tx.get(ref); p = row.data().publication;
      if (!p || p.phase === 'blocked' || p.phase === 'attached' || p.leaseUntil > Date.now() || p.nextCheckAt > Date.now()) return;
      if (reviewHash(row.data()) !== p.reviewHash) throw Error('The reviewed animation changed.');
      p = {...p, owner, phase:'uploading', leaseUntil:Date.now()+13*60000, updatedAt:Date.now(), error:null};
      tx.update(ref, {publication:p});
    });
    if (p?.owner !== owner) return {ok:true, cached:true};
    const save = async patch => {
      p = {...p, ...patch, updatedAt:Date.now()};
      await D.fb().db.runTransaction(async tx => {
        const row = await tx.get(ref);
        if (row.data()?.publication?.owner !== owner || reviewHash(row.data()) !== p.reviewHash) throw Error('The publication changed.');
        tx.update(ref, {publication:clone(p)});
      });
    };
    try {
      await D.assertTarget(job);
      for (const video of p.videos) {
        if (video.resourceName) continue;
        const variant = selection(job).find(v => v.key === video.key);
        const bytes = await D.loadVideo(variant.asset);
        if (!bytes.length || bytes.length > 100000000) throw Error('The saved video size is invalid.');
        if (!video.sessionUrl) {
          if (video.inFlight) throw Error('The previous upload has no confirmed session receipt. Reconcile it before another upload.');
          video.inFlight = 'start'; await save({videos:p.videos});
          const session = await D.startUpload({bytes:bytes.length, title:job.title+' · '+variant.format, description:job.title+' — '+job.destination});
          video.sessionUrl = uploadUrl(session.url); video.inFlight = null; video.state = 'PENDING'; await save({videos:p.videos});
        }
        // Query the durable session before sending any bytes, including on recovery.
        const state = await D.queryUpload(uploadUrl(video.sessionUrl));
        if (state.resourceName) {video.resourceName = state.resourceName; video.state = 'UPLOADED'; video.inFlight = null; await save({videos:p.videos}); continue;}
        if (video.inFlight === 'finalize') throw Error('Google may have accepted the video, but its final receipt is unavailable. Reconcile this session before retrying.');
        const offset = Number(state.offset);
        if (!Number.isInteger(offset) || offset < 0 || offset > bytes.length) throw Error('Google returned an invalid upload offset.');
        video.inFlight = 'finalize'; await save({videos:p.videos});
        const result = await D.finishUpload(uploadUrl(video.sessionUrl), bytes.subarray(offset), offset);
        if (!/^customers\/\d+\/youTubeVideoUploads\/[a-zA-Z0-9_-]+$/.test(result.resourceName || '')) throw Error('Google returned no complete upload receipt.');
        video.resourceName = result.resourceName; video.state = 'UPLOADED'; video.inFlight = null; await save({videos:p.videos});
      }
      await save({phase:'processing'});
      for (const video of p.videos) {
        const result = await D.uploadState(video.resourceName);
        if (!result || result.resourceName !== video.resourceName) throw Error('Google has not returned the exact uploaded video.');
        video.state = result.state; video.videoId = result.videoId || null;
        await save({videos:p.videos});
        if (['FAILED','REJECTED','UNAVAILABLE'].includes(result.state)) throw Error('Google video processing returned '+result.state+' for '+video.key+'.');
      }
      if (p.videos.some(v => v.state !== 'PROCESSED' || !/^[a-zA-Z0-9_-]{11}$/.test(v.videoId || ''))) {
        await save({phase:'processing', nextCheckAt:Date.now()+60000, leaseUntil:0}); return {ok:true, processing:true};
      }
      await D.assertTarget(job);
      // A missing attachment response is never blindly replayed.
      if (p.attachmentInFlight) throw Error('The prior Google asset attachment needs reconciliation before another mutation.');
      await save({attachmentInFlight:true});
      const receipt = await D.attach(job, p.videos);
      await save({phase:'attached', attachmentInFlight:false, attachmentReceipt:receipt, attachedAt:Date.now(), leaseUntil:0});
      return {ok:true, attached:true};
    } catch (e) {
      await save({phase:'blocked', leaseUntil:0, error:String(e.message || e).slice(0,1000)});
      return {ok:false, error:e.message};
    }
  }
  async function verify(input) {
    const {ref, job} = await context(input), p = job.publication;
    if (!p || p.phase !== 'attached') return {ok:true, publication:safePublication(p)};
    let acquired=false;
    await D.fb().db.runTransaction(async tx=>{const current=await tx.get(ref),live=current.data().publication;if(live.verification?.checkedAt>Date.now()-300000||live.verifyLeaseUntil>Date.now())return;tx.update(ref,{'publication.verifyLeaseUntil':Date.now()+120000});acquired=true;});
    if(!acquired)return {ok:true,cached:true,publication:safePublication((await ref.get()).data().publication)};
    try{const verification={...await D.verify(job,p.videos),checkedAt:Date.now()};await ref.update({'publication.verification':verification,'publication.updatedAt':Date.now(),'publication.verifyLeaseUntil':0});return {ok:true,publication:safePublication({...p,verification})};}
    catch(e){await ref.update({'publication.verifyLeaseUntil':Date.now()+60000});throw e;}
  }

  async function prepareMerchant(input){
    const {ref,job}=await context(input);selection(job);
    if(!require('./googleAdsAdMotion').qualityPass(job.quality))throw Error('The video set must pass its saved quality target first.');
    const p=job.publication;if(p?.phase!=='attached'||p.merchant?.inFlight)throw Error('Finish or reconcile the Google video upload first.');
    const plan=await D.prepareMerchant(job,p.videos);
    if(plan.requiresIdentity)throw Error('More than one Merchant market or language matches. Select the exact product feed in the design workspace first.');
    const merchant={phase:'review',reviewHash:plan.reviewHash,plan,videoReviewHash:p.reviewHash,updatedAt:Date.now()};
    await D.fb().db.runTransaction(async tx=>{const row=await tx.get(ref),current=row.data();if(current.publication?.reviewHash!==p.reviewHash||current.publication?.merchant?.inFlight||reviewHash(current)!==reviewHash(job))throw Error('The saved videos changed.');tx.update(ref,{'publication.merchant':merchant});});
    return {ok:true,merchant:safePublication({...p,merchant}).merchant};
  }
  async function publishMerchant(input){
    const {ref,job}=await context(input),m=job.publication?.merchant;
    if(!require('./googleAdsAdMotion').qualityPass(job.quality))throw Error('The video set must pass its saved quality target first.');
    if(!m||m.reviewHash!==input.merchantReviewHash||m.videoReviewHash!==reviewHash(job))throw Error('Review the exact Merchant video links first.');
    if(m.phase==='accepted')return {ok:true,cached:true};
    if(m.inFlight||m.phase==='blocked')throw Error('The prior Merchant update needs reconciliation before another request.');
    await D.fb().db.runTransaction(async tx=>{const row=await tx.get(ref),current=row.data();if(!require('./googleAdsAdMotion').qualityPass(current.quality)||current.publication?.phase!=='attached'||current.publication?.merchant?.reviewHash!==m.reviewHash||current.publication?.merchant?.inFlight||current.publication?.merchant?.phase!=='review'||reviewHash(current)!==m.videoReviewHash)throw Error('The Merchant video review changed.');tx.update(ref,{'publication.merchant':{...m,inFlight:true}});});
    try{const receipt=await D.publishMerchant(m.plan);const merchant={...m,inFlight:false,phase:receipt.status==='VALIDATED'?'validated':'accepted',receipt,updatedAt:Date.now()};await ref.update({'publication.merchant':merchant});return {ok:true,merchant:safePublication({...job.publication,merchant}).merchant};}
    catch(e){await ref.update({'publication.merchant':{...m,phase:'blocked',inFlight:true,error:String(e.message||e),updatedAt:Date.now()}});throw e;}
  }

  return {start, run, verify,prepareMerchant,publishMerchant};
}
module.exports = {createPublicationService, reviewHash, safePublication, uploadUrl, selection,nativeCompatibility};

