// Durable optional video generation. Google publication remains an explicit action.
const crypto=require('crypto'),fs=require('fs/promises'),path=require('path'),os=require('os'),{promisify}=require('util'),execFile=promisify(require('child_process').execFile),sharp=require('sharp');
const policy=require('../../brites-ad-format-policy'),{MODEL,SECONDS,OUTPUT_USD_PER_SECOND,requestBody,outputVideo}=require('./googleAdsGeminiVideo'),MAX_BYTES=100000000;
const clone=v=>JSON.parse(JSON.stringify(v)),hash=v=>crypto.createHash('sha256').update(typeof v==='string'?v:JSON.stringify(v)).digest('hex');
const qualityPass=q=>q?.pass===true&&q?.productFaithful===true&&q?.mobileReadable===true&&Number.isFinite(q?.score)&&q.score>=97&&q.score<=100;
function binary(){return process.env.BRITES_FFMPEG_PATH||require('@ffmpeg-installer/ffmpeg').path;}
async function ffmpeg(args){try{return await execFile(binary(),['-hide_banner','-loglevel','error','-nostdin',...args],{timeout:180000,maxBuffer:1000000});}catch(e){throw new Error('Video rendering failed: '+String(e.stderr||e.message).slice(0,500));}}
function cropFilter(width,height,zoom=1){const z=Math.max(1,Math.min(1.08,Number(zoom)||1));return `scale=${Math.ceil(width*z/2)*2}:${Math.ceil(height*z/2)*2}:force_original_aspect_ratio=increase,crop=${width}:${height},setsar=1,fps=24`;}
async function renderVariants(bytes,orientation,plan={}){
 const dir=await fs.mkdtemp(path.join(os.tmpdir(),'brites-motion-'));try{
  const photoMotion=String(plan.motionMode||'').startsWith('photograph'),closeFrame=plan.motionMode==='photograph-close',input=path.join(dir,photoMotion?'source.jpg':'source.mp4');await fs.writeFile(input,bytes);const out=[];
  for(const device of ['mobile','desktop'])for(const format of policy.video.formats){
   if((format.key==='portrait'?'portrait':format.key==='landscape'?'landscape':device==='mobile'?'portrait':'landscape')!==orientation)continue;
   const layout=(plan.layouts||[]).find(l=>l.device===device&&l.family===format.key),name=device+'_'+format.key,file=path.join(dir,name+'.mp4'),zoom=Math.min(1.08,Number(layout?.zoom)||1); // Reframing never sacrifices product identity for arbitrary zoom.
   // Preserve the complete still, including edge details, during the 2% push.
   // Padding absorbs the movement; no generated frames can invent jewelry.
   const inset=closeFrame?({portrait:1.08,square:1.25,landscape:1.4}[format.key]):1;
   const closeFilter=`scale=${Math.ceil(format.width*inset/2)*2}:${Math.ceil(format.height*inset/2)*2}:force_original_aspect_ratio=increase,crop=${format.width}:${format.height},zoompan=z='1+0.02*on/239':x='(iw-iw/zoom)/2':y='(ih-ih/zoom)/2':d=240:s=${format.width}x${format.height}:fps=24,setsar=1`;
   const filter=closeFrame?closeFilter:photoMotion?`scale=${Math.floor(format.width*.95/2)*2}:${Math.floor(format.height*.95/2)*2}:force_original_aspect_ratio=decrease,pad=${format.width}:${format.height}:(ow-iw)/2:(oh-ih)/2:color=0xf7f2ea,zoompan=z='1+0.02*on/239':x='(iw-iw/zoom)/2':y='(ih-ih/zoom)/2':d=240:s=${format.width}x${format.height}:fps=24,setsar=1`:cropFilter(format.width,format.height,zoom)+',tpad=stop_mode=clone:stop_duration=10';
   await ffmpeg(['-y','-i',input,'-vf',filter,'-t',String(SECONDS),'-an','-c:v','libx264','-preset','veryfast','-crf','20','-pix_fmt','yuv420p','-movflags','+faststart','-metadata','comment='+(photoMotion?'Photograph motion':'AI-generated product video')+'; Brites Jewelry',file]);
   const video=await fs.readFile(file);if(video.length>MAX_BYTES)throw new Error('The rendered clip exceeds the bounded video size.');
   const frames=[];for(const second of [.5,5,9.5]){const frame=path.join(dir,name+'_'+second+'.jpg');await ffmpeg(['-y','-ss',String(second),'-i',file,'-frames:v','1','-vf','scale=640:640:force_original_aspect_ratio=decrease',frame]);frames.push(await fs.readFile(frame));}
   out.push({key:name,device,format:format.key,width:format.width,height:format.height,seconds:SECONDS,bytes:video,frames});
  }return out;
 }finally{await fs.rm(dir,{recursive:true,force:true});}
}
function createMotionService(D){
 const jobs=ref=>ref.collection('motionJobs'),scope=(w,p)=>{if(w.archivedAt||String(w.settings.productId)!==String(p.productId)||w.settings.groupRef!==p.groupRef)throw new Error('This animated ad belongs to another product or group.');};
 async function start(input){
  if(input.resumeJobId){
   const {ref,w}=await D.context(input.workspaceId);scope(w,input);if(!/^motion_[a-f0-9]{40}$/.test(input.resumeJobId))throw Error('Choose a saved animation to resume.');
   const row=await jobs(ref).doc(input.resumeJobId).get();if(!row.exists||row.data().resetAt)throw Error('The saved animation was not found.');const job=row.data();scope(w,job);
   if(job.quality&&job.phase!=='ready')throw Error('This animation needs a reviewed correction, not another generation attempt.');
   return {ok:true,workspaceId:input.workspaceId,jobId:job.id,queued:job.phase!=='ready'};
  }
  if(input.photoMotionOf)return photograph(input);
  if(input.repairOf)return repair(input);
  const {ref,w,products}=await D.context(input.workspaceId);if(!input.fromEditorWorker)scope(w,input);
  const matching=(!input.editorJobId&&!input.fromEditorWorker)?(await ref.collection('editorAIJobs').get()).docs.map(d=>d.data()).filter(j=>j.phase==='ready'&&!j.resetAt&&j.scope.productId===input.productId&&j.scope.groupRef===input.groupRef).sort((a,b)=>b.createdAt-a.createdAt)[0]:null;const editorId=input.editorJobId||matching?.id||w.editorAI?.id;if(!/^eai_[a-f0-9]{40}$/.test(editorId||''))throw new Error('Run AI Design to prepare a product scene before animating it.');
  const editor=ref.collection('editorAIJobs').doc(editorId),[record,saved,request]=await Promise.all([editor.get(),editor.collection('data').doc('result').get(),editor.collection('data').doc('request').get()]);
  if(!record.exists||record.data().phase!=='ready'||!saved.exists||!saved.data().responsive||!request.exists)throw new Error('The product scene is still being designed. Its animation will follow when ready.');
  const editorScope=record.data().scope;if(!input.fromEditorWorker)scope(w,editorScope);if(w.archivedAt)throw Error('This ad was deleted.');const product=products.find(p=>String(p.id)===String(editorScope.productId)),group=(w.context.groups||[]).find(g=>g.ref===editorScope.groupRef);if(!product||!group)throw Error('The saved animation product or group is no longer available.');const result=saved.data(),id='motion_'+hash(editorId).slice(0,40),target=jobs(ref).doc(id);
  await D.fb().db.runTransaction(async tx=>{const current=await tx.get(ref),existing=await tx.get(target);if(current.data().archivedAt)throw Error('This ad was deleted.');if(!input.fromEditorWorker)scope(current.data(),input);if(existing.exists)return;
   tx.set(target,{id,editorJobId:editorId,workspaceId:input.workspaceId,productId:product.id,groupRef:group.ref,title:product.title,destination:product.url,phase:'queued',createdAt:Date.now(),updatedAt:Date.now(),leaseUntil:0,owner:null,progress:{pct:0,label:'Preparing mobile-first motion'},plan:result.responsive.plan,sourceImages:result.sources,originalSources:request.data().identitySources||request.data().sources,masters:{},variants:[],estimatedUsd:2*SECONDS*OUTPUT_USD_PER_SECOND,costEstimated:true,provider:MODEL,seconds:SECONDS,inFlight:null,error:null});
  });return {ok:true,workspaceId:input.workspaceId,jobId:id,queued:true};
 }
 async function repair(input){
  const {ref,w}=await D.context(input.workspaceId);scope(w,input);if(!/^motion_[a-f0-9]{40}$/.test(input.repairOf||''))throw Error('Choose the saved animation to repair.');
  const parentRef=jobs(ref).doc(input.repairOf),id='motion_'+hash('repair:'+input.repairOf).slice(0,40),target=jobs(ref).doc(id);
  await D.fb().db.runTransaction(async tx=>{const current=await tx.get(ref),source=await tx.get(parentRef),existing=await tx.get(target);scope(current.data(),input);if(!source.exists)throw Error('The original animation was not found.');const parent=source.data();scope(current.data(),parent);
   if(parent.resetAt||parent.repairOf||!['needs_attention','ready'].includes(parent.phase)||!parent.quality||qualityPass(parent.quality)||parent.inFlight||parent.leaseUntil>Date.now())throw Error('Only a completed, failed quality review can receive this bounded repair.');
   if(input.repairReviewHash!==hash({id:parent.id,quality:parent.quality}))throw Error('The animation review changed. Refresh before approving a repair.');if(existing.exists)return;
   tx.set(target,{...clone(parent),id,repairOf:parent.id,repairIssues:parent.quality.issues||[],createdAt:Date.now(),updatedAt:Date.now(),phase:'queued',owner:null,leaseUntil:0,inFlight:null,masters:{},variants:[],quality:null,publication:null,error:null,estimatedUsd:2*SECONDS*OUTPUT_USD_PER_SECOND,progress:{pct:0,label:'Preparing one reviewed animation repair'}});
  });return {ok:true,workspaceId:input.workspaceId,jobId:id,queued:true};
 }
 async function photograph(input){
  const {ref,w}=await D.context(input.workspaceId);scope(w,input);if(!/^motion_[a-f0-9]{40}$/.test(input.photoMotionOf||''))throw Error('Choose the saved animation first.');
  const target=jobs(ref).doc('motion_'+hash('photograph:'+input.photoMotionOf).slice(0,40));
  await D.fb().db.runTransaction(async tx=>{
   const current=await tx.get(ref),source=await tx.get(jobs(ref).doc(input.photoMotionOf)),existing=await tx.get(target);scope(current.data(),input);if(!source.exists)throw Error('The original animation was not found.');const parent=source.data();scope(current.data(),parent);
   if(parent.resetAt||parent.motionMode==='photograph-close'||!['needs_attention','ready'].includes(parent.phase)||!parent.quality||qualityPass(parent.quality)||parent.inFlight||parent.leaseUntil>Date.now())throw Error('Photograph motion requires a completed animation review needing correction.');
   if(input.repairReviewHash!==hash({id:parent.id,quality:parent.quality}))throw Error('The animation review changed. Refresh first.');if(existing.exists)return;
   if(!parent.sourceImages?.length)throw Error('The saved product photographs are missing.');
   tx.set(target,{...clone(parent),id:target.id,photoMotionOf:parent.id,motionMode:parent.motionMode==='photograph'?'photograph-close':'photograph',provider:'saved-photograph',createdAt:Date.now(),updatedAt:Date.now(),phase:'queued',owner:null,leaseUntil:0,inFlight:null,masters:{},variants:[],quality:null,publication:null,error:null,estimatedUsd:0,progress:{pct:0,label:'Preparing motion from the saved photograph'}});
  });return {ok:true,workspaceId:input.workspaceId,jobId:target.id,queued:true};
 }
 async function status(input){
  const {ref,w}=await D.context(input.workspaceId);scope(w,input);let job,workspaceId=input.workspaceId;
  if(input.jobId){const s=await jobs(ref).doc(input.jobId).get();job=s.exists?s.data():null;}
  else{const rows=await jobs(ref).get();job=rows.docs.map(d=>d.data()).filter(j=>j.productId===input.productId&&j.groupRef===input.groupRef&&!j.resetAt).sort((a,b)=>b.createdAt-a.createdAt)[0];}
  // A Google publication creates a new design version. Its existing paid films
  // remain owned by their original workspace; discover them without copying
  // jobs, losing provider receipts, or starting another generation.
  if(!job&&!input.jobId&&D.relatedContexts&&w.context?.campaignId){
   const candidates=[];
   for(const prior of await D.relatedContexts(w,input.workspaceId)){
    if(prior.w.archivedAt||String(prior.w.context?.campaignId)!==String(w.context.campaignId)||String(prior.w.settings?.productId)!==String(input.productId)||prior.w.settings?.groupRef!==input.groupRef)continue;
    const rows=await jobs(prior.ref).get();
    for(const row of rows.docs){const j=row.data();if(!j.resetAt&&j.workspaceId===prior.ref.id&&String(j.productId)===String(input.productId)&&j.groupRef===input.groupRef)candidates.push(j);}
   }
   job=candidates.sort((a,b)=>b.createdAt-a.createdAt)[0];if(job)workspaceId=job.workspaceId;
  }
  if(!job||job.resetAt)return {ok:true,phase:'idle',variants:[],formats:policy.video.formats};scope(w,job);
  return {ok:true,workspaceId,fromEarlierVersion:workspaceId!==input.workspaceId,jobId:job.id,phase:job.phase,error:job.error,startedAt:job.createdAt,progress:job.progress,estimatedUsd:job.estimatedUsd,costEstimated:true,canResume:job.phase!=='ready'&&!job.quality&&(!job.inFlight||job.inFlight.key==='quality'),quality:job.quality||null,qualityTarget:97,qualityTargetMet:qualityPass(job.quality),motionMode:job.motionMode||'generated',canPhotoMotion:job.motionMode!=='photograph-close'&&['needs_attention','ready'].includes(job.phase)&&!!job.quality&&!qualityPass(job.quality)&&!job.inFlight,canRepair:!String(job.motionMode||'').startsWith('photograph')&&!job.repairOf&&['needs_attention','ready'].includes(job.phase)&&!!job.quality&&!qualityPass(job.quality)&&!job.inFlight,repairReviewHash:job.quality?hash({id:job.id,quality:job.quality}):null,repairOf:job.repairOf||null,publication:require('./googleAdsMotionPublication').safePublication(job.publication),reviewHash:job.phase==='ready'?require('./googleAdsMotionPublication').reviewHash(job):null,formats:policy.video.formats,variants:await Promise.all((job.variants||[]).map(async v=>({...v,url:await D.signVideo(v.asset),posterUrl:v.poster?await D.signVideo(v.poster):null}))),masterProgress:Object.entries(job.masters||{}).map(([format,m])=>({format,status:m.status,progress:m.progress||0}))};
 }
 async function run(input){
  const {ref,w}=await D.context(input.workspaceId),target=jobs(ref).doc(input.jobId),owner=crypto.randomUUID();let job;
  await D.fb().db.runTransaction(async tx=>{const row=await tx.get(target);if(!row.exists)throw new Error('The animated request was not found.');job=row.data();if(w.archivedAt)throw Error('This ad was deleted.');if(job.resetAt||job.phase==='ready'||job.leaseUntil>Date.now())return;job={...job,owner,leaseUntil:Date.now()+13*60000,phase:'running',updatedAt:Date.now(),error:null};tx.update(target,job);});
  if(!job||job.owner!==owner)return {ok:true,cached:true};
  const save=async patch=>{Object.assign(job,patch,{updatedAt:Date.now()});await D.fb().db.runTransaction(async tx=>{const current=await tx.get(target);if(current.data()?.owner!==owner||current.data()?.resetAt)throw new Error('The animated job was reset or changed.');tx.update(target,clone(job));});};
  try{
   const sourceFor=orientation=>job.sourceImages.find(s=>orientation==='portrait'?s.height>s.width:s.width>s.height)||job.sourceImages[0];
   if(String(job.motionMode||'').startsWith('photograph')){
    for(const orientation of ['portrait','landscape'])job.masters[orientation]={id:'photograph_'+orientation,status:'completed',progress:100,source:sourceFor(orientation).asset};
    await save({masters:job.masters});
   }
   for(const orientation of ['portrait','landscape']){
    if(job.masters[orientation]?.id)continue;if(job.inFlight&&job.inFlight.key!=='quality')throw new Error('The last video request has no confirmed provider ID. Its receipt must be reconciled before another paid request.');
    const size=orientation==='portrait'?'720x1280':'1280x720',[width,height]=size.split('x').map(Number),image=await sharp(await D.loadAsset(sourceFor(orientation).asset)).resize({width:1280,height:1280,fit:'inside',withoutEnlargement:true}).jpeg({quality:92}).toBuffer();
    const direction=job.plan.motion?.[orientation==='portrait'?'mobilePrompt':'desktopPrompt']||job.plan.imageDirections?.[0]?.composition||'';
    const correction=job.repairOf?' Correct the rejected first attempt: '+(job.repairIssues||[]).join(' ')+' Keep the supplied surface texture and leaf recess exactly unchanged. Lock the product angle and geometry; permit only a subtle camera push. Do not add props, cylindrical supports or any new background objects. Preserve the full jewelry within the central 80 percent, with the product occupying about 70 to 75 percent of frame height. Use the existing soft background.':'';
    const prompt=`Create a restrained 10-second jewelry product film for ${job.title}. This reference defines the exact product. Preserve its silhouette, chain, cutouts, proportions, finish and physical scale in EVERY frame. ${direction}. ${policy.principles.join(' ')} Use a single continuous unbroken shot, no scene cuts. Use a stable macro or gentle camera move; no spinning that invents unseen surfaces, no morphing, new pieces, extra stones, engraving or changing clasps. Product visible from the opening frame; hold a calm final view. Keep the complete jewelry inside the central square-safe area throughout. No typography, labels, logos, buttons, watermarks or speech. An adult model may appear if present in the supplied reference; preserve natural anatomy and exact jewelry. Scene intent: ${job.plan.motion?.concept||job.plan.rationale}.${correction}`;
    const requestId=crypto.randomUUID();await save({inFlight:{orientation,requestId,at:Date.now()},progress:{pct:orientation==='portrait'?5:10,label:'Starting '+orientation+' product animation'}});
    const data=await D.videoRequest('interactions','POST',requestBody(image,prompt,orientation));
    if(!/^v1_[a-zA-Z0-9_.:-]+$/.test(data.id||''))throw new Error('The video provider returned no durable job ID.');
    job.masters[orientation]={id:data.id,status:data.status,progress:data.status==='completed'?100:0,output:outputVideo(data),size,requestId,estimatedUsd:SECONDS*OUTPUT_USD_PER_SECOND};await save({masters:job.masters,inFlight:null});
   }
   const deadline=Date.now()+9*60000;
   while(Object.values(job.masters).some(m=>m.status!=='completed')){
    for(const [orientation,m]of Object.entries(job.masters)){if(m.status==='completed')continue;const data=await D.videoRequest('interactions/'+m.id);Object.assign(m,{status:data.status,progress:data.status==='completed'?100:0,output:outputVideo(data),error:data.error||null});if(['failed','cancelled','requires_action'].includes(data.status))throw new Error(orientation+' animation failed: '+(data.error?.message||'Provider rejected the scene.'));}
    const pct=15+Math.round(Object.values(job.masters).reduce((n,m)=>n+(m.status==='completed'?100:m.progress||0),0)/200*50);await save({masters:job.masters,progress:{pct,label:'Generating motion · portrait and landscape'}});
    if(Date.now()>deadline){await save({phase:'queued',leaseUntil:0});return {ok:true,continue:true,workspaceId:input.workspaceId,jobId:job.id};}
    if(Object.values(job.masters).some(m=>m.status!=='completed'))await new Promise(r=>setTimeout(r,15000));
   }
   for(const [orientation,m]of Object.entries(job.masters)){
    if(!String(job.motionMode||'').startsWith('photograph')&&!m.asset){const bytes=await D.videoContent(m.output);m.asset=await D.saveVideo(job.workspaceId,bytes,job.id+'_'+orientation,{mimeType:'video/mp4',seconds:SECONDS,...Object.fromEntries(m.size.split('x').map((v,i)=>[i?'height':'width',Number(v)]))});await save({masters:job.masters});}
    if(job.variants.filter(v=>v.master===orientation).length===3)continue;
    await save({progress:{pct:orientation==='portrait'?68:77,label:'Rendering '+orientation+' crops for mobile and desktop'}});
    const variants=await (D.renderVariants||renderVariants)(String(job.motionMode||'').startsWith('photograph')?await D.loadAsset(m.source):await D.loadVideo(m.asset),orientation,{...job.plan,motionMode:job.motionMode});
    for(const v of variants){if(job.variants.some(x=>x.key===v.key))continue;const asset=await D.saveVideo(job.workspaceId,v.bytes,job.id+'_'+v.key,{mimeType:'video/mp4',width:v.width,height:v.height,seconds:v.seconds}),poster=await D.saveVideo(job.workspaceId,v.frames[0],job.id+'_'+v.key+'_poster',{mimeType:'image/jpeg'}),frames=[];for(let i=0;i<v.frames.length;i++)frames.push(await D.saveVideo(job.workspaceId,v.frames[i],job.id+'_'+v.key+'_frame'+i,{mimeType:'image/jpeg'}));job.variants.push({key:v.key,device:v.device,format:v.format,width:v.width,height:v.height,seconds:v.seconds,master:orientation,asset,poster,frames});await save({variants:job.variants});}
   }
   if(!job.quality){
    const receipt=target.collection('receipts').doc('quality'),prior=await receipt.get();
    if(job.inFlight&&!prior.exists)throw new Error('The saved video review has no confirmed response.');
    await save({progress:{pct:90,label:'Checking jewelry identity, motion and all six exports'},inFlight:{key:'quality',requestId:job.inFlight?.requestId||crypto.randomUUID()}});
    const refs=await Promise.all(job.originalSources.map(s=>D.loadAsset(s.asset))),frames=await Promise.all(job.variants.flatMap(v=>v.frames).map(a=>D.loadVideo(a)));
    const quality=await D.reviewImages(refs[0],frames,{copy:job.plan.nativeCopy,keywords:[],product:job.title,inputCoverage:{usedProductImages:refs.length},motionReview:'These are opening, middle and closing frames of six device/ratio variants. Check the entire jewelry stays visible and physically consistent. Camera motion may change perspective, never identity.'},refs,job.inFlight.requestId,{...(prior.exists?{rawResponse:prior.data().response}:{}),onResponse:response=>receipt.set({response,at:Date.now()})});
    await save({quality,inFlight:null,estimatedUsd:(String(job.motionMode||'').startsWith('photograph')?0:2*SECONDS*OUTPUT_USD_PER_SECOND)+(Number(quality.estimatedUsd)||0)});
   }
   if(job.quality.productFaithful!==true||job.quality.mobileReadable!==true||job.quality.pass!==true||!Number.isFinite(job.quality.score)||job.quality.score<97||job.quality.score>100)throw new Error('Animated jewelry has not met the 97/100 quality target: '+(job.quality.issues||[]).join(' '));
   await save({phase:'ready',leaseUntil:0,completedAt:Date.now(),error:null,progress:{pct:100,label:'Six animated variants ready to review'}});return {ok:true,workspaceId:job.workspaceId,jobId:job.id};
  }catch(e){await save({phase:'needs_attention',leaseUntil:0,error:String(e.message||e).slice(0,800),...(e.definiteResponse?{inFlight:null}:{}),progress:{pct:job.progress?.pct||0,label:'Animation paused; saved images and completed video work retained'}});return {ok:false,error:e.message};}
 }
 return {start,status,run};
}
module.exports={createMotionService,renderVariants,cropFilter,MODEL,SECONDS};

