// Durable optional video generation. Google publication remains an explicit action.
const crypto=require('crypto'),fs=require('fs/promises'),path=require('path'),os=require('os'),{promisify}=require('util'),execFile=promisify(require('child_process').execFile),sharp=require('sharp');
const policy=require('../../brites-ad-format-policy'),{MODEL,SECONDS,OUTPUT_USD_PER_SECOND,requestBody,outputVideo}=require('./googleAdsGeminiVideo'),MAX_BYTES=100000000;
const clone=v=>JSON.parse(JSON.stringify(v)),hash=v=>crypto.createHash('sha256').update(typeof v==='string'?v:JSON.stringify(v)).digest('hex');
function binary(){return process.env.BRITES_FFMPEG_PATH||require('@ffmpeg-installer/ffmpeg').path;}
async function ffmpeg(args){try{return await execFile(binary(),['-hide_banner','-loglevel','error','-nostdin',...args],{timeout:180000,maxBuffer:1000000});}catch(e){throw new Error('Video rendering failed: '+String(e.stderr||e.message).slice(0,500));}}
function cropFilter(width,height,zoom=1){const z=Math.max(1,Math.min(1.08,Number(zoom)||1));return `scale=${Math.ceil(width*z/2)*2}:${Math.ceil(height*z/2)*2}:force_original_aspect_ratio=increase,crop=${width}:${height},setsar=1,fps=24`;}
async function renderVariants(bytes,orientation,plan={}){
 const dir=await fs.mkdtemp(path.join(os.tmpdir(),'brites-motion-'));try{
  const input=path.join(dir,'source.mp4');await fs.writeFile(input,bytes);const out=[];
  for(const device of ['mobile','desktop'])for(const format of policy.video.formats){
   if((format.key==='portrait'?'portrait':format.key==='landscape'?'landscape':device==='mobile'?'portrait':'landscape')!==orientation)continue;
   const layout=(plan.layouts||[]).find(l=>l.device===device&&l.family===format.key),name=device+'_'+format.key,file=path.join(dir,name+'.mp4'),zoom=Math.min(1.08,Number(layout?.zoom)||1); // Reframing never sacrifices product identity for arbitrary zoom.
   await ffmpeg(['-y','-i',input,'-vf',cropFilter(format.width,format.height,zoom)+',tpad=stop_mode=clone:stop_duration=10','-t',String(SECONDS),'-an','-c:v','libx264','-preset','veryfast','-crf','20','-pix_fmt','yuv420p','-movflags','+faststart','-metadata','comment=AI-generated product video; Brites Jewelry',file]);
   const video=await fs.readFile(file);if(video.length>MAX_BYTES)throw new Error('The rendered clip exceeds the bounded video size.');
   const frames=[];for(const second of [.5,5,9.5]){const frame=path.join(dir,name+'_'+second+'.jpg');await ffmpeg(['-y','-ss',String(second),'-i',file,'-frames:v','1','-vf','scale=640:640:force_original_aspect_ratio=decrease',frame]);frames.push(await fs.readFile(frame));}
   out.push({key:name,device,format:format.key,width:format.width,height:format.height,seconds:SECONDS,bytes:video,frames});
  }return out;
 }finally{await fs.rm(dir,{recursive:true,force:true});}
}
function createMotionService(D){
 const jobs=ref=>ref.collection('motionJobs'),scope=(w,p)=>{if(w.archivedAt||String(w.settings.productId)!==String(p.productId)||w.settings.groupRef!==p.groupRef)throw new Error('This animated ad belongs to another product or group.');};
 async function start(input){
  const {ref,w,products}=await D.context(input.workspaceId);if(!input.fromEditorWorker)scope(w,input);
  const matching=(!input.editorJobId&&!input.fromEditorWorker)?(await ref.collection('editorAIJobs').get()).docs.map(d=>d.data()).filter(j=>j.phase==='ready'&&!j.resetAt&&j.scope.productId===input.productId&&j.scope.groupRef===input.groupRef).sort((a,b)=>b.createdAt-a.createdAt)[0]:null;const editorId=input.editorJobId||matching?.id||w.editorAI?.id;if(!/^eai_[a-f0-9]{40}$/.test(editorId||''))throw new Error('Run AI Design to prepare a product scene before animating it.');
  const editor=ref.collection('editorAIJobs').doc(editorId),[record,saved,request]=await Promise.all([editor.get(),editor.collection('data').doc('result').get(),editor.collection('data').doc('request').get()]);
  if(!record.exists||record.data().phase!=='ready'||!saved.exists||!saved.data().responsive||!request.exists)throw new Error('The product scene is still being designed. Its animation will follow when ready.');
  const editorScope=record.data().scope;if(!input.fromEditorWorker)scope(w,editorScope);if(w.archivedAt)throw Error('This ad was deleted.');const product=products.find(p=>String(p.id)===String(editorScope.productId)),group=(w.context.groups||[]).find(g=>g.ref===editorScope.groupRef);if(!product||!group)throw Error('The saved animation product or group is no longer available.');const result=saved.data(),id='motion_'+hash(editorId).slice(0,40),target=jobs(ref).doc(id);
  await D.fb().db.runTransaction(async tx=>{const current=await tx.get(ref),existing=await tx.get(target);if(current.data().archivedAt)throw Error('This ad was deleted.');if(!input.fromEditorWorker)scope(current.data(),input);if(existing.exists)return;
   tx.set(target,{id,editorJobId:editorId,workspaceId:input.workspaceId,productId:product.id,groupRef:group.ref,title:product.title,destination:product.url,phase:'queued',createdAt:Date.now(),updatedAt:Date.now(),leaseUntil:0,owner:null,progress:{pct:0,label:'Preparing mobile-first motion'},plan:result.responsive.plan,sourceImages:result.sources,originalSources:request.data().sources,masters:{},variants:[],estimatedUsd:2*SECONDS*OUTPUT_USD_PER_SECOND,costEstimated:true,provider:MODEL,seconds:SECONDS,inFlight:null,error:null});
  });return {ok:true,workspaceId:input.workspaceId,jobId:id,queued:true};
 }
 async function status(input){
  const {ref,w}=await D.context(input.workspaceId);scope(w,input);let job;
  if(input.jobId){const s=await jobs(ref).doc(input.jobId).get();job=s.exists?s.data():null;}
  else{const rows=await jobs(ref).get();job=rows.docs.map(d=>d.data()).filter(j=>j.productId===input.productId&&j.groupRef===input.groupRef&&!j.resetAt).sort((a,b)=>b.createdAt-a.createdAt)[0];}
  if(!job||job.resetAt)return {ok:true,phase:'idle',variants:[],formats:policy.video.formats};scope(w,job);
  return {ok:true,jobId:job.id,phase:job.phase,error:job.error,startedAt:job.createdAt,progress:job.progress,estimatedUsd:job.estimatedUsd,costEstimated:true,canResume:job.phase!=='ready'&&(!job.inFlight||job.inFlight.key==='quality'),quality:job.quality||null,publication:job.publication||null,formats:policy.video.formats,variants:await Promise.all((job.variants||[]).map(async v=>({...v,url:await D.signVideo(v.asset),posterUrl:v.poster?await D.signVideo(v.poster):null}))),masterProgress:Object.entries(job.masters||{}).map(([format,m])=>({format,status:m.status,progress:m.progress||0}))};
 }
 async function run(input){
  const {ref,w}=await D.context(input.workspaceId),target=jobs(ref).doc(input.jobId),owner=crypto.randomUUID();let job;
  await D.fb().db.runTransaction(async tx=>{const row=await tx.get(target);if(!row.exists)throw new Error('The animated request was not found.');job=row.data();if(w.archivedAt)throw Error('This ad was deleted.');if(job.resetAt||job.phase==='ready'||job.leaseUntil>Date.now())return;job={...job,owner,leaseUntil:Date.now()+13*60000,phase:'running',updatedAt:Date.now(),error:null};tx.update(target,job);});
  if(!job||job.owner!==owner)return {ok:true,cached:true};
  const save=async patch=>{Object.assign(job,patch,{updatedAt:Date.now()});await D.fb().db.runTransaction(async tx=>{const current=await tx.get(target);if(current.data()?.owner!==owner||current.data()?.resetAt)throw new Error('The animated job was reset or changed.');tx.update(target,clone(job));});};
  try{
   const sourceFor=orientation=>job.sourceImages.find(s=>orientation==='portrait'?s.height>s.width:s.width>s.height)||job.sourceImages[0];
   for(const orientation of ['portrait','landscape']){
    if(job.masters[orientation]?.id)continue;if(job.inFlight&&job.inFlight.key!=='quality')throw new Error('The last video request has no confirmed provider ID. Its receipt must be reconciled before another paid request.');
    const size=orientation==='portrait'?'720x1280':'1280x720',[width,height]=size.split('x').map(Number),image=await sharp(await D.loadAsset(sourceFor(orientation).asset)).resize({width:1280,height:1280,fit:'inside',withoutEnlargement:true}).jpeg({quality:92}).toBuffer();
    const direction=job.plan.motion?.[orientation==='portrait'?'mobilePrompt':'desktopPrompt']||job.plan.imageDirections?.[0]?.composition||'';
    const prompt=`Create a restrained 10-second jewelry product film for ${job.title}. This reference defines the exact product. Preserve its silhouette, chain, cutouts, proportions, finish and physical scale in EVERY frame. ${direction}. ${policy.principles.join(' ')} Use a single continuous unbroken shot, no scene cuts. Use a stable macro or gentle camera move; no spinning that invents unseen surfaces, no morphing, new pieces, extra stones, engraving or changing clasps. Product visible from the opening frame; hold a calm final view. Keep the complete jewelry inside the central square-safe area throughout. No typography, labels, logos, buttons, watermarks or speech. An adult model may appear if present in the supplied reference; preserve natural anatomy and exact jewelry. Scene intent: ${job.plan.motion?.concept||job.plan.rationale}.`;
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
    if(!m.asset){const bytes=await D.videoContent(m.output);m.asset=await D.saveVideo(job.workspaceId,bytes,job.id+'_'+orientation,{mimeType:'video/mp4',seconds:SECONDS,...Object.fromEntries(m.size.split('x').map((v,i)=>[i?'height':'width',Number(v)]))});await save({masters:job.masters});}
    if(job.variants.filter(v=>v.master===orientation).length===3)continue;
    await save({progress:{pct:orientation==='portrait'?68:77,label:'Rendering '+orientation+' crops for mobile and desktop'}});
    const variants=await (D.renderVariants||renderVariants)(await D.loadVideo(m.asset),orientation,job.plan);
    for(const v of variants){if(job.variants.some(x=>x.key===v.key))continue;const asset=await D.saveVideo(job.workspaceId,v.bytes,job.id+'_'+v.key,{mimeType:'video/mp4',width:v.width,height:v.height,seconds:v.seconds}),poster=await D.saveVideo(job.workspaceId,v.frames[0],job.id+'_'+v.key+'_poster',{mimeType:'image/jpeg'}),frames=[];for(let i=0;i<v.frames.length;i++)frames.push(await D.saveVideo(job.workspaceId,v.frames[i],job.id+'_'+v.key+'_frame'+i,{mimeType:'image/jpeg'}));job.variants.push({key:v.key,device:v.device,format:v.format,width:v.width,height:v.height,seconds:v.seconds,master:orientation,asset,poster,frames});await save({variants:job.variants});}
   }
   if(!job.quality){
    const receipt=target.collection('receipts').doc('quality'),prior=await receipt.get();
    if(job.inFlight&&!prior.exists)throw new Error('The saved video review has no confirmed response.');
    await save({progress:{pct:90,label:'Checking jewelry identity, motion and all six exports'},inFlight:{key:'quality',requestId:job.inFlight?.requestId||crypto.randomUUID()}});
    const refs=await Promise.all(job.originalSources.map(s=>D.loadAsset(s.asset))),frames=await Promise.all(job.variants.flatMap(v=>v.frames).map(a=>D.loadVideo(a)));
    const quality=await D.reviewImages(refs[0],frames,{copy:job.plan.nativeCopy,keywords:[],product:job.title,inputCoverage:{usedProductImages:refs.length},motionReview:'These are opening, middle and closing frames of six device/ratio variants. Check the entire jewelry stays visible and physically consistent. Camera motion may change perspective, never identity.'},refs,job.inFlight.requestId,{...(prior.exists?{rawResponse:prior.data().response}:{}),onResponse:response=>receipt.set({response,at:Date.now()})});
    await save({quality,inFlight:null,estimatedUsd:2*SECONDS*OUTPUT_USD_PER_SECOND+(Number(quality.estimatedUsd)||0)});
   }
   if(!job.quality.productFaithful)throw new Error('Animated jewelry needs review: '+(job.quality.issues||[]).join(' '));
   await save({phase:'ready',leaseUntil:0,completedAt:Date.now(),error:null,progress:{pct:100,label:'Six animated variants ready to review'}});return {ok:true,workspaceId:job.workspaceId,jobId:job.id};
  }catch(e){await save({phase:'needs_attention',leaseUntil:0,error:String(e.message||e).slice(0,800),...(e.definiteResponse?{inFlight:null}:{}),progress:{pct:job.progress?.pct||0,label:'Animation paused; saved images and completed video work retained'}});return {ok:false,error:e.message};}
 }
 return {start,status,run};
}
module.exports={createMotionService,renderVariants,cropFilter,MODEL,SECONDS};
