const assert=require('node:assert/strict'),sharp=require('sharp'),fs=require('node:fs/promises'),os=require('node:os'),path=require('node:path');
const references=require('../../netlify/functions/googleAdsMotionReferences');
const {createMotionService,renderVariants,motionPrompt,motionRequest}=require('../../netlify/functions/googleAdsAdMotion');
const {referenceRequestBody}=require('../../netlify/functions/googleAdsGeminiVideo');
const {selection}=require('../../netlify/functions/googleAdsMotionPublication');
const clone=x=>JSON.parse(JSON.stringify(x));let checks=0;
function check(value,message){assert(value,message);checks++;}
function memory(){
 const docs=new Map(),doc=p=>({id:p.split('/').pop(),path:p,get:async()=>({exists:docs.has(p),data:()=>clone(docs.get(p)||null)}),set:async v=>docs.set(p,clone(v)),update:async v=>docs.set(p,{...docs.get(p),...clone(v)}),collection:n=>collection(p+'/'+n)}),collection=p=>({doc:n=>doc(p+'/'+n),get:async()=>({docs:[...docs].filter(([k])=>k.startsWith(p+'/')&&!k.slice(p.length+1).includes('/')).map(([k,v])=>({id:k.split('/').pop(),data:()=>clone(v)}))})});
 return {docs,db:{collection,runTransaction:fn=>fn({get:r=>r.get(),set:(r,v)=>r.set(v),update:(r,v)=>r.update(v),delete:r=>docs.delete(r.path)})}};
}
const copy={headline:'Your everyday favourites',shortHeadline:'Made for you',description:'Discover the collection.',cta:'Shop now'};
const good=()=>({pass:true,productFaithful:true,exactProductIdentity:true,mobileReadable:true,footageLettering:false,multipleProducts:false,score:98,issues:[],formatIdentity:Object.fromEntries(references.KEYS.map(k=>[k,{sameOutline:true,noAddedDetail:true,noMissingDetail:true,sameFeatures:true}]))});
const geometry={jewelryType:'Earrings',assembly:'One pair, two matching earrings',outline:'Elongated asymmetric oval with a pointed lower right corner',negativeSpace:'One round opening below each hook',surfaceDetails:'AB engraving centred on each face',attachment:'One separate wire hook on each earring',chain:'None',material:'Gold metal with a flat face and thin edge',proportions:'Each oval is three times as tall as wide',physicalSize:'Unknown; preserve reference scale cues',supportedViews:'Front and shallow left angle only'};
const direction=()=>({...Object.fromEntries(['rationale','setting','props','lighting','camera','opening','middle','ending','portrait','square','landscape','identity','limitations'].map(k=>[k,'The supplied earrings sway gently together in bright natural light.'])),geometry:clone(geometry),shapePlan:{contour:'From the top hook, follow the narrow asymmetric oval to its pointed lower right corner and rounded left edge.',features:[{name:'Earrings',count:2,positions:'One on each side of the assembly',appearance:'Matching elongated asymmetric ovals with one opening and AB engraving per earring'}],confusionRisks:'The opening below each hook is not an extra jump ring.'},sceneStory:{motif:'Asymmetric engraved ovals',connection:'A curved pale stone and folded linen echo the oval curves without covering the engraving.',propMotion:'The loose linen edge moves gently beside the pair.'},supportingReferenceIndices:[2]});
const response=value=>({output:[{type:'message',content:[{type:'output_text',text:JSON.stringify(value)}]}]});
(async()=>{
 const photo=async colour=>sharp(Buffer.from('<svg xmlns="http://www.w3.org/2000/svg" width="480" height="480"><rect width="480" height="480" fill="white"/><g fill="'+colour+'"><path d="M150 85Q205 150 175 375L120 350Q95 130 150 85Z"/><path d="M310 85Q365 150 335 375L280 350Q255 130 310 85Z"/></g><g fill="white"><circle cx="150" cy="120" r="9"/><circle cx="310" cy="120" r="9"/></g><g font-size="16"><text x="137" y="230">AB</text><text x="297" y="230">AB</text></g></svg>')).jpeg({quality:94}).toBuffer();
 const photos=[await photo('#c99b3b'),await photo('#bbbbbb'),await sharp(await photo('#c99b3b')).extend({left:1,right:0,top:0,bottom:0,background:'white'}).jpeg().toBuffer()];
 const prepared=await references.prepare(photos[0]);check(prepared.bytes.equals(photos[0])&&prepared.mimeType==='image/jpeg','opaque original reaches generation byte-for-byte, without masking');
 const png=await sharp(photos[0]).png().toBuffer();check((await references.prepare(png)).bytes.equals(png),'PNG originals also stay unchanged');
 const request=motionRequest({motionMode:references.MODE,referencePolicy:references.POLICY,title:'Engraved earrings'},photos.map(bytes=>({bytes,mimeType:'image/jpeg'})));
 check(request.model===require('../../netlify/functions/googleAdsAdDesignResearch').MODEL,'planner uses the existing connected vision model');
 const providerPlan=require('../../netlify/functions/_googleAdsClaude').fromResponsesRequest(request);check(providerPlan.messages.flatMap(m=>m.content).filter(c=>c.type==='image').length===3,'existing provider bridge forwards every actual image');
 check(request.input[1].content.filter(c=>c.type==='input_image').length===3,'planner sees every candidate original, not just a title');
 check(request.input[0].content.includes('including their location, strokes, spelling and orientation'),'engraving is explicitly preserved despite the ban on added scene text');
 for(const g of [geometry,{...geometry,jewelryType:'Necklace',assembly:'One pendant and chain',chain:'Fine link chain'}, {...geometry,jewelryType:'Ring',assembly:'One ring',attachment:'None',material:'Rounded solid silver band'}]){
  const d=references.validate({...direction(),geometry:g},3),prompt=motionPrompt({motionMode:references.MODE,referencePolicy:references.POLICY,creativeDirection:d,generationReferences:[{},{}]},'square');
  check(prompt.includes(JSON.stringify(g))&&prompt.includes('<IMAGE_REF_0>@Image1 <IMAGE_REF_1>@Image2'),'each jewelry type retains its own complete geometry and explicit media references');
 }
 assert.throws(()=>references.validate({...direction(),geometry:{outline:'Oval'}},3),/missing/);checks++;
 check(references.validate({...direction(),supportingReferenceIndices:[3]},3).supportingReferenceIndices.length===0,'out-of-range optional references are omitted without replacing the primary');
 const body=referenceRequestBody([prepared],'reference film','portrait');check(body.generation_config.video_config.task==='reference_to_video'&&body.response_format.aspect_ratio==='9:16','provider receives explicit subject-reference mode and correct orientation');
 check(Buffer.from(body.input[0].data,'base64').equals(photos[0]),'actual provider payload retains original photograph');
 const temp=await fs.mkdtemp(path.join(os.tmpdir(),'reference-motion-test-')),exec=require('node:util').promisify(require('node:child_process').execFile),bin=require('@ffmpeg-installer/ffmpeg').path;
 let film;try{await exec(bin,['-y','-loglevel','error','-f','lavfi','-i','testsrc2=size=1280x720:rate=24:duration=10','-c:v','libx264','-threads','2','-crf','20',path.join(temp,'film.mp4')]);film=await fs.readFile(path.join(temp,'film.mp4'));}finally{await fs.rm(temp,{recursive:true,force:true});}
 const rendered={};
 for(const orientation of ['portrait','square','landscape']){
  const [v]=await renderVariants(film,orientation,{motionMode:references.MODE,pipelineVersion:6,renderVersion:14,copy,composition:{x:.4,y:.45,w:.25,h:.4,body:null}});
  rendered[orientation]=v;console.log('Rendered '+orientation+' generated-video path');check(v.frames.length===6&&v.seconds===10&&v.bytes.length>10000&&!v.integrity,'real generated footage survives the caption renderer, with no photographic product layer');
  check(!v.frames[0].equals(v.frames[4]),'output retains moving video content');
 }
 async function setup(review=good){
  const f=memory(),ref=f.db.collection('Workspaces').doc('design_test'),scope={workspaceId:'design_test',productId:'p',groupRef:'g',generationMode:'reference_to_video'},id='eai_'+'a'.repeat(40),blobs=new Map(),calls={provider:0,render:[],review:0,direction:0,layout:0};
  const sources=photos.map((b,i)=>({id:'original'+i,source:{kind:'product',productId:'p'},productId:'p',asset:{path:'original'+i,hash:references.hash(b)}}));
  await ref.set({settings:scope,context:{groups:[{ref:'g'}]}});const editor=ref.collection('editorAIJobs').doc(id);
  await editor.set({id,scope,phase:'ready',createdAt:1});await editor.collection('data').doc('request').set({identitySources:sources,sources});
  await editor.collection('data').doc('result').set({responsive:{plan:{copy,nativeCopy:{headlines:[copy.headline],descriptions:[copy.description]}}},sources:[{asset:{path:'generated-artwork'}}]});
  const D={fb:()=>f,context:async()=>({ref,w:(await ref.get()).data(),products:[{id:'p',title:'Engraved earrings',url:'https://example.test/earrings'}]}),identityFor:async()=>sources,loadAsset:async a=>photos[Number(a.path.replace('original',''))],
   videoRequest:async(route,method,body)=>{calls.provider++;assert.equal(method,'POST');assert.equal(body.generation_config.video_config.task,'reference_to_video');const images=body.input.filter(i=>i.type==='image');assert.equal(images.length,2);assert(Buffer.from(images[0].data,'base64').equals(photos[0]));assert(Buffer.from(images[1].data,'base64').equals(photos[2]));assert(body.input.at(-1).text.includes('One pair, two matching earrings'));return {id:'v1_reference_'+calls.provider,status:'completed',steps:[{type:'model_output',content:[{type:'video',data:film.toString('base64')}]}]};},videoContent:async()=>film,
   planMotion:async req=>{if(req.text.format.name==='brites_referenced_motion'){calls.direction++;return response(direction());}calls.layout++;return response(Object.fromEntries(['portrait','square','landscape'].map(o=>[o,{bounds:[.4,.45,.25,.4],complete:true,confidence:1,note:'Complete supplied pair'}])));},
   sampleMotionFrames:async(b,o)=>{assert(b.equals(film));return [{orientation:o,second:1,bytes:photos[0]}];},
   saveVideo:async(w,b,key,info)=>{const a={path:key,hash:references.hash(b),...info};blobs.set(key,b);return a;},loadVideo:async a=>blobs.get(a.path),signVideo:async a=>'https://example.test/'+a.path,
   renderVariants:async(b,o,p)=>{assert(b.equals(film));assert.equal(p.motionMode,references.MODE);assert(!p.backgroundVideo);calls.render.push(o);return [rendered[o]];},
   reviewImages:async(primary,frames,brief,refs)=>{calls.review++;assert.equal(refs.length,2,'rejected silver variant is excluded from review too');assert.equal(brief.geometry.assembly,geometry.assembly);assert(brief.motionReview.includes('a pair of earrings is not an extra product'));return review();}};
  return {f,ref,scope,D,calls,service:createMotionService(D)};
 }
 const e=await setup(),s=await e.service.start(e.scope),run=await e.service.run({...e.scope,jobId:s.jobId});check(run.ok,run.error);
 const target=e.ref.collection('motionJobs').doc(s.jobId),job=(await target.get()).data(),status=await e.service.status(e.scope);
 check(status.phase==='ready'&&status.referenceGuided&&!status.productProtected&&status.qualityTargetMet&&status.reviewHash,'successful generated reference job is ready without claiming deterministic pixel preservation');
 check(e.calls.provider===3&&e.calls.review===1&&e.calls.direction===1,'three independently generated films share one upstream geometry brief and one existing final review');
 check(job.generationReferences.map(r=>r.source.path).join(',')==='original0,original2','only primary and selected matching variant condition every movie');
 check(selection(job).length===3,'reviewed generated films qualify for explicit publication');
 await e.service.run({...e.scope,jobId:s.jobId});check(e.calls.provider===3&&e.calls.render.length===3,'completed resume never buys or renders duplicate videos');
 const redo=await e.service.start({...e.scope,redoOf:job.id,redoFormat:'landscape',confirmRedo:true});check((await e.service.run({...e.scope,jobId:redo.jobId})).ok,'one-format redo remains usable');
 const redone=(await e.ref.collection('motionJobs').doc(redo.jobId).get()).data();check(e.calls.provider===4&&redone.variants.find(v=>v.key==='mobile_square').asset.hash===job.variants.find(v=>v.key==='mobile_square').asset.hash,'redo buys only chosen film and keeps other exports');
 for(const mutate of [j=>delete j.referencePolicy,j=>j.variants[0].fidelity.referenceHash='f'.repeat(64),j=>j.variants[1].asset.hash='changed',j=>j.variants[2].frames.pop(),j=>j.quality.exactProductIdentity=false]){const bad=clone(job);mutate(bad);assert.throws(()=>selection(bad),/references|identity/i);checks++;}
 assert.throws(()=>references.assertVideo(job.variants[0],Buffer.from('substituted')),/differs/);checks++;
 const failed=await setup(()=>({...good(),exactProductIdentity:false,score:100})),fsJob=await failed.service.start(failed.scope);await failed.service.run({...failed.scope,jobId:fsJob.jobId});const held=await failed.service.status(failed.scope);check(held.phase==='needs_attention'&&!held.reviewHash,'aesthetic score never overrides product identity failure');
 const malformed=await setup(),planner=malformed.D.planMotion;let preparationCalls=0;malformed.D.planMotion=async req=>++preparationCalls===1?response({outline:'generic'}):planner(req);const ms=await malformed.service.start(malformed.scope);
 check((await malformed.service.run({...malformed.scope,jobId:ms.jobId})).ok&&malformed.calls.provider===3,'incomplete preparation is corrected automatically before generating the three films');
 const paused=await setup(),renderer=paused.D.renderVariants;paused.D.renderVariants=async()=>{throw Error('render interrupted');};const ps=await paused.service.start(paused.scope);await paused.service.run({...paused.scope,jobId:ps.jobId});paused.D.renderVariants=renderer;await paused.service.start({...paused.scope,resumeJobId:ps.jobId});check((await paused.service.run({...paused.scope,jobId:ps.jobId})).ok&&paused.calls.provider===3,'resume keeps saved provider films');
 const changed=await setup();changed.D.renderVariants=async()=>{throw Error('render interrupted');};const cs=await changed.service.start(changed.scope);await changed.service.run({...changed.scope,jobId:cs.jobId});changed.D.loadAsset=async()=>photos[1];await changed.service.start({...changed.scope,resumeJobId:cs.jobId});const rejected=await changed.service.run({...changed.scope,jobId:cs.jobId});check(!rejected.ok&&/photos changed/.test(rejected.error)&&changed.calls.provider===3,'changing originals cannot silently mix references with paid films');
 const legacy=clone(job);legacy.id='motion_'+'c'.repeat(40);legacy.motionMode='protected-product-motion';legacy.pipelineVersion=5;legacy.sourcePreparation='transparent-product-required';delete legacy.referencePolicy;const legacyRef=e.ref.collection('motionJobs').doc(legacy.id);await legacyRef.set(legacy);
 const migrated=await e.service.start({...e.scope,rerunOf:legacy.id});check(migrated.jobId!==legacy.id&&(await e.service.run({...e.scope,jobId:migrated.jobId})).ok,'old cutout jobs rebuild from opaque originals without masks or manual uploads');check(JSON.stringify((await legacyRef.get()).data())===JSON.stringify(legacy),'earlier paid files remain saved');
 console.log('PASS '+checks+' original-reference generation checks, including three real 10-second renders');
 require('./suite-guard.cjs').done();
})().catch(e=>{console.error(e.stack);process.exitCode=1;});
