// Explicit, durable publication of reviewed videos to a paused, product-only PMax group.
// Upload receipts, YouTube processing, asset attachment and ad policy are distinct states.
const crypto = require('crypto');
const digest = value => crypto.createHash('sha256').update(JSON.stringify(value)).digest('hex');
const clone = value => JSON.parse(JSON.stringify(value));
const KEYS = ['mobile_portrait', 'mobile_square', 'desktop_landscape'];
function selection(job) {
  if (job.phase !== 'ready' || job.quality?.pass !== true || job.quality?.productFaithful !== true) throw Error('The animation must pass its jewelry quality review before Google publication.');
  const variants = KEYS.map(key => job.variants.find(v => v.key === key));
  if (variants.some(v => !v?.asset?.hash || !v.asset.path || v.seconds !== 10)) throw Error('Three complete reviewed video formats are required.');
  return variants;
}
function reviewHash(job) {
  return digest({jobId:job.id, productId:job.productId, groupRef:job.groupRef, destination:job.destination, variants:selection(job).map(v => ({key:v.key, asset:v.asset}))});
}
function safePublication(p) {
  if (!p) return null;
  return {phase:p.phase, error:p.error || null, updatedAt:p.updatedAt, attachedAt:p.attachedAt || null,
    videos:(p.videos || []).map(v => ({key:v.key, state:v.state, resourceName:v.resourceName || null, videoId:v.videoId || null})),
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
    if(job.publication?.phase!=='attached'&&(!Number.isFinite(job.quality?.score)||job.quality.score<97||job.quality.score>100||job.quality.mobileReadable!==true))throw Error('The saved animation has not met the 97/100 quality target. Improve the product scene before a new Google upload.');
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
      if (!p || p.phase === 'blocked' || p.phase === 'attached' || p.leaseUntil > Date.now()) return;
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
        await save({phase:'processing', leaseUntil:0}); return {ok:true, processing:true};
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
    const verification = await D.verify(job, p.videos);
    await ref.update({'publication.verification':verification, 'publication.updatedAt':Date.now()});
    return {ok:true, publication:safePublication({...p, verification})};
  }
  return {start, run, verify};
}
module.exports = {createPublicationService, reviewHash, safePublication, uploadUrl, selection};

