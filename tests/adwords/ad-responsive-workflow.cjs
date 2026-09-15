const fs=require('node:fs'),vm=require('node:vm'),path=require('node:path'),assert=require('node:assert/strict'),sharp=require('sharp');
// Reuse the in-memory service fixture without running its separate regression suite.
const fixture=path.join(__dirname,'ad-design-workflow.cjs'),source=fs.readFileSync(fixture,'utf8').split('(async()=>{const e=await setup();')[0];
const ctx=vm.createContext({require:require('node:module').createRequire(fixture),__dirname,process,Buffer,console,Date,setTimeout,clearTimeout});vm.runInContext(source+'\nglobalThis.setupFixture=setup;',ctx);
const {plan}=require('./ad-responsive.cjs');let n=0;const ok=(v,m)=>{assert(v,m);n++};
async function setup({generatedSource=false,autoProofs=true,layer=null,makeSource=null}={}){const e=await ctx.setupFixture(),bytes=await sharp({create:{width:1024,height:1024,channels:3,background:'#d1b284'}}).jpeg().toBuffer();e.D.fullSourceBytes=async()=>bytes;
 let source=await e.svc.editorSource({workspaceId:e.id,productId:plan.productId,groupRef:plan.groupRef,source:{kind:'product',productId:plan.productId,imageId:'img1'}});
 e.D.research.collect=async()=>({hash:'facts',researchCompletedAt:Date.now(),sourceBindings:{landingUrl:'https://britesjewelry.com/products/duck'},sources:[{id:'product:'+plan.productId,status:'available',data:{title:'Duck necklace',description:'Duck pendant necklace'}}],warnings:[]});
 e.D.responses=async args=>{if(args.text?.format?.name==='brites_subject_focus'){e.calls.focus=(e.calls.focus||0)+1;return {model:'gpt-6-astra',output_text:JSON.stringify({...({landscape:{x:.58,y:.08,width:.29,height:.8},square:{x:.3,y:.04,width:.4,height:.38},portrait:{x:.28,y:.04,width:.44,height:.5},tall:{x:.29,y:.18,width:.42,height:.42},banner:{x:.04,y:.08,width:.23,height:.84},slim:{x:.29,y:.18,width:.42,height:.42}}[plan.scenePlans?.[e.calls.focus-1]?.key]||{x:.46,y:.65,width:.08,height:.18}),confident:true}),estimatedUsd:.03,costEstimated:false};}e.calls.responses++;return {model:'gpt-6-astra',output_text:JSON.stringify(plan),estimatedUsd:.1,costEstimated:false}};
 e.D.generateImage=async({format})=>{e.calls.images++;return {bytes:await sharp(bytes).resize(format.width,format.height).jpeg().toBuffer(),estimatedUsd:.2,costEstimated:false}};
 e.D.reviewImages=async()=>{e.calls.quality++;return {pass:true,productFaithful:true,mobileReadable:true,score:92,issues:[],estimatedUsd:.1,costEstimated:false}};
 e.D.cropImage=async(bytes,format)=>{const b=require('../../brites-ad-responsive').boards.find(b=>b.key===format);return {bytes:await sharp(bytes).resize(b.width,b.height,{fit:'cover'}).jpeg().toBuffer(),width:b.width,height:b.height}};
 if(makeSource)source=await makeSource(e,source);
 if(generatedSource){const prior={...source,id:'scene_prior',source:{kind:'library',imageId:'generated_prior'},asset:await e.D.saveAsset(e.id,await sharp({create:{width:1024,height:1024,channels:3,background:'#ff0000'}}).jpeg().toBuffer(),'prior',{width:1024,height:1024})};e.f.docs.set(e.p+'/editorSources/'+prior.id,prior);source.id=prior.id;}
 const input={workspaceId:e.id,productId:plan.productId,groupRef:plan.groupRef,device:'shared',artboard:{key:'square',width:2048,height:2048},requestId:'fixture_new_scene',mode:'design',generateScene:true,document:{objects:[{id:'photo_layer',type:'Image',sourceKey:source.id,left:0,top:0,width:1024,height:1024,scaleX:2,scaleY:2,...(layer||{})}]},screenshotDataUrl:'data:image/jpeg;base64,'+bytes.toString('base64')};
 const started=await e.svc.editorAIStart(input);
 const originalRun=e.svc.editorAIRun;
 if(autoProofs)e.svc.editorAIRun=async args=>{let out=await originalRun(args),status=await e.svc.editorAIStatus({...input,jobId:started.jobId});if(status.phase==='awaiting_review'){const c=status.candidate,boards=[{...c.artboard,key:'active'},...require('../../brites-ad-responsive').variants.map(b=>({...b,key:b.device+'_'+b.key}))],proofs=[];for(const b of boards){const scale=Math.min(1,960/Math.max(b.width,b.height));proofs.push({key:b.key,width:b.width,height:b.height,renderCheck:{version:1,visiblePhotoFraction:.3},dataBase64:(await sharp(bytes).resize(Math.round(b.width*scale),Math.round(b.height*scale)).jpeg().toBuffer()).toString('base64')});}await e.svc.editorAIResume({...input,jobId:started.jobId,candidateHash:c.candidateHash,reviewProofs:proofs});out=await originalRun(args);}return out;};
 return {...e,input,jobId:started.jobId};}
(async()=>{
 for(const failure of [{status:'failed',error:{code:'server_error'}},{status:'incomplete',incomplete_details:{reason:'max_output_tokens'}},{status:'completed',output_text:'{broken'}]){
 const failedProvider=await setup(),originalResponses=failedProvider.D.responses;let failOnce=true;
 failedProvider.D.responses=async args=>{if(failOnce){failOnce=false;return failure;}return originalResponses(args);};
 await failedProvider.svc.editorAIRun({workspaceId:failedProvider.id,jobId:failedProvider.jobId});
 await failedProvider.svc.editorAIResume({...failedProvider.input,jobId:failedProvider.jobId});
 await failedProvider.svc.editorAIRun({workspaceId:failedProvider.id,jobId:failedProvider.jobId});
 ok((await failedProvider.svc.editorAIStatus({...failedProvider.input,jobId:failedProvider.jobId})).phase==='ready','explicit resume escapes a confirmed failed provider receipt');
 ok([...failedProvider.f.docs.keys()].some(k=>k.includes('/responseHistory/response_')),'failed provider response is archived');
 }
 const e=await setup();let result=await e.svc.editorAIRun({workspaceId:e.id,jobId:e.jobId});assert(result.ok,result.error);
 ok(e.calls.focus===1,'one saved charm localization supplies every responsive crop');
 const status=await e.svc.editorAIStatus({...e.input,jobId:e.jobId});ok(status.qualityTarget===92&&status.result.quality.score===92,'92-point batch passes through review and apply without the old 97-point gate');ok(status.result.sources[0].url&&status.result.document.objects[0].sourceKey.startsWith('scene_'),'new scene has a durable signed source');ok(e.calls.images===1&&e.calls.responses===1&&e.calls.quality===1,'one master generation reused across all formats');ok(status.result.publicationImages.length===3&&status.result.responsive.variants.length===26,'three clean assets plus device-specific layout coverage');
 await e.svc.editorAIApply({...e.input,jobId:e.jobId});const applied=e.f.docs.get(e.p);ok(applied.placements.length===6&&applied.messaging.copy.headlines.length===5,'six placements and native messaging apply together');const revision=applied.revision;await e.svc.editorAIApply({...e.input,jobId:e.jobId});ok(e.f.docs.get(e.p).revision===revision,'reapply is idempotent');await e.svc.editorAIRun({workspaceId:e.id,jobId:e.jobId});ok(e.calls.images===1&&e.calls.focus===1,'completed job never generates or locates the same image twice');
 const allSizes=await e.svc.editorAIStatus({...e.input,artboard:{key:'portrait',width:1638,height:2048},allSizes:true,includeReview:true});ok(allSizes.jobId===e.jobId&&allSizes.reviewProofs.length===27,'all-size gallery remains accessible from other artboards after application');
 const engine=require('../../brites-ad-responsive');
 for(const board of engine.boards)for(const device of ['shared','desktop','mobile']){
  const state=await e.svc.editorState({...e.input,artboard:board,device});
  ok(state.design?.responsiveDraft&&state.sources.length>0,'reopened '+device+' '+board.key+' retains paid semantic layout');
  const expected=engine.document(status.result.responsive.plan,engine.selectImage(status.result.responsive.plan,status.result.responsive.images,board),board,device==='desktop'?'desktop':'mobile');
  assert.deepEqual(JSON.parse(JSON.stringify(state.design.document)),JSON.parse(JSON.stringify(expected)));n++;
 }
 const currentRenderer=engine.document;engine.document=()=>{throw Error('New template must not rebuild saved reviewed artwork');};
 const frozenScope={...e.input,device:'desktop',artboard:engine.boards.find(b=>b.key==='portrait')};
 ok((await e.svc.editorState(frozenScope)).design.document.objects.length>0,'reopening uses frozen format document even after the template renderer changes');
 ok((await e.svc.editorResponsiveState(frozenScope)).document.objects.length>0,'explicit restore uses frozen reviewed coordinates');engine.document=currentRenderer;
 const preservedScope={...e.input,device:'shared',artboard:engine.boards.find(b=>b.key==='landscape')},preserved=await e.svc.editorState(preservedScope),custom=JSON.parse(JSON.stringify(preserved.design.document));custom.objects.push({type:'Textbox',text:'My saved wording',left:20,top:20});
 await e.svc.editorSave({...preservedScope,document:custom,name:'Operator design',expectedRevision:0});const reopened=await e.svc.editorState(preservedScope);ok(reopened.design.name==='Operator design'&&!reopened.design.responsiveDraft&&reopened.design.document.objects.some(o=>o.text==='My saved wording'),'explicit saved artwork wins over generated fallback');
 const recovered=await e.svc.editorResponsiveState(preservedScope);ok(recovered.document.objects.some(o=>o.sourceKey)&&!recovered.document.objects.some(o=>o.text==='My saved wording'),'explicit restore returns original paid semantic layout');ok((await e.svc.editorState(preservedScope)).design.name==='Operator design','requesting recovery does not overwrite saved artwork');ok(e.calls.images===1&&e.calls.responses===1,'layout recovery makes no paid provider request');await assert.rejects(()=>e.svc.editorResponsiveState({...preservedScope,productId:'other'}),/product|changed/i);n++;
 const nextId='design_next_version',nextPath=e.p.slice(0,e.p.lastIndexOf('/')+1)+nextId,priorWorkspace=JSON.parse(JSON.stringify(e.f.docs.get(e.p)));
 e.f.docs.set(nextPath,{...priorWorkspace,workspaceId:nextId,sourceVersion:2,context:{...priorWorkspace.context,legacyEditorWorkspaceIds:[e.id]}});
 const nextInput={...e.input,workspaceId:nextId,artboard:engine.boards.find(b=>b.key==='portrait')};
 const inherited=await e.svc.editorState(nextInput);ok(inherited.design.responsiveDraft&&inherited.design.legacyWorkspaceId===e.id&&inherited.sources.length>0,'a missing artboard recovers its paid plan across Google ad versions');
 const restoredNext=await e.svc.editorResponsiveState(nextInput);ok(restoredNext.jobId===e.jobId&&restoredNext.sources.length>0,'explicit AI layout restore finds the original applied job in the same campaign');
 await e.svc.editorSave({...nextInput,document:restoredNext.document,name:'Recovered portrait',expectedRevision:0});ok((await e.svc.editorState(nextInput)).design.name==='Recovered portrait','recovered source references support a normal editable save in the new workspace');
 ok(e.calls.images===1&&e.calls.responses===1&&e.calls.quality===1,'cross-version recovery reuses all paid results without another provider request');
 for(const patch of [{archivedAt:1},{context:{...priorWorkspace.context,campaignId:'other'}},{context:{...priorWorkspace.context,groups:[]}}]){
  e.f.docs.set(e.p,{...priorWorkspace,...patch});await assert.rejects(()=>e.svc.editorResponsiveState(nextInput),/No applied AI layout/);n++;
 }
 e.f.docs.set(e.p,priorWorkspace);
 const stale=await setup();await stale.svc.editorAIRun({workspaceId:stale.id,jobId:stale.jobId});stale.f.docs.get(stale.p).messaging={copy:{headlines:['New user edit']}};await assert.rejects(()=>stale.svc.editorAIApply({...stale.input,jobId:stale.jobId}),/changed while AI/);ok(stale.f.docs.get(stale.p).messaging.copy.headlines[0]==='New user edit','newer user copy retained');
 const unknown=await setup();unknown.D.generateImage=async()=>{unknown.calls.images++;throw Error('network outcome unknown')};await unknown.svc.editorAIRun({workspaceId:unknown.id,jobId:unknown.jobId});const paused=await unknown.svc.editorAIStatus({...unknown.input,jobId:unknown.jobId});ok(paused.phase==='needs_attention'&&!paused.canRetry,'unknown image charge protected despite text receipt');await assert.rejects(()=>unknown.svc.editorAIResume({...unknown.input,jobId:unknown.jobId}),/paid request/);ok(unknown.calls.images===1,'no blind image replacement');
 const legacy=await setup(),legacyPath=legacy.p+'/editorAIJobs/'+legacy.jobId+'/data/';await legacy.svc.editorAIRun({workspaceId:legacy.id,jobId:legacy.jobId});
 const generated=legacy.f.docs.get(legacyPath+'scene_0');legacy.f.docs.set(legacyPath+'scene_repair',generated);legacy.f.docs.set(legacyPath+'scene_quality',{pass:false,score:90,issues:['Minor groove difference']});legacy.f.docs.set(legacyPath+'scene_repair_quality',{pass:false,score:92,issues:['Minor bevel difference']});
 legacy.f.docs.set(legacyPath+'ad_quality_v1',{score:78.8,pass:false,issues:['Small CTA']});legacy.f.docs.set(legacyPath+'ad_proofs_v1',{candidateHash:'historical'});
 legacy.f.docs.set(legacyPath+'ad_quality_v3',{score:54.4,pass:false,issues:['Missing product photograph']});legacy.f.docs.set(legacyPath+'ad_proofs_v3',{candidateHash:'missing-photo-history'});
 legacy.f.docs.delete(legacyPath+'ad_proofs_v11');legacy.f.docs.delete(legacyPath+'ad_quality_v11');legacy.f.docs.get(legacy.p+'/editorAIJobs/'+legacy.jobId).phase='needs_attention';
 await legacy.svc.editorAIRun({workspaceId:legacy.id,jobId:legacy.jobId});ok(legacy.calls.images===1&&legacy.calls.responses===1,'new rubric reuses saved correction and plan without regeneration');ok(legacy.f.docs.get(legacyPath+'scene_repair_quality').score===92,'former detail review remains an immutable history record');
 ok(legacy.f.docs.get(legacyPath+'ad_quality_v1').score===78.8&&legacy.f.docs.get(legacyPath+'ad_proofs_v1').candidateHash==='historical','new layout review preserves former weighted review and its original proofs');
 ok(legacy.f.docs.get(legacyPath+'ad_quality_v3').score===54.4&&legacy.f.docs.get(legacyPath+'ad_proofs_v3').candidateHash==='missing-photo-history','new photo-verified review retains the failed rendering and its honest score');
 const revised=await setup();await revised.svc.editorAIRun({workspaceId:revised.id,jobId:revised.jobId});
 const revisedPath=revised.p+'/editorAIJobs/'+revised.jobId,originalResponse=JSON.stringify(revised.f.docs.get(revisedPath+'/data/response'));
 revised.f.docs.set(revisedPath+'/data/ad_quality_v2',{score:83.4,pass:false,issues:['Generic copy']});revised.f.docs.delete(revisedPath+'/data/ad_quality_v11');revised.f.docs.delete(revisedPath+'/data/ad_proofs_v11');revised.f.docs.get(revisedPath).phase='needs_attention';
 const revisedPlan=JSON.parse(JSON.stringify(plan));revisedPlan.copy.description='A duck pendant to make your own.';
 revised.D.responses=async()=>{revised.calls.responses++;return {model:'gpt-6-astra',output_text:JSON.stringify(revisedPlan),estimatedUsd:.2,costEstimated:false}};
 await revised.svc.editorAIRun({workspaceId:revised.id,jobId:revised.jobId});
 const revisedStatus=await revised.svc.editorAIStatus({...revised.input,jobId:revised.jobId});
 ok(revisedStatus.phase==='ready'&&revisedStatus.result.responsive.plan.copy.description===revisedPlan.copy.description,'saved review feedback produces a separately validated copy revision');
 ok(revised.calls.images===1&&revised.calls.responses===2&&revised.calls.quality===2,'copy repair purchases no replacement photographs');
 ok(JSON.stringify(revised.f.docs.get(revisedPath+'/data/response'))===originalResponse&&revised.f.docs.get(revisedPath+'/data/ad_quality_v2').score===83.4,'copy repair retains original response and historical weighted review');
 revised.f.docs.get(revisedPath).phase='needs_attention';await revised.svc.editorAIRun({workspaceId:revised.id,jobId:revised.jobId});ok(revised.calls.responses===2&&revised.calls.images===1&&revised.calls.quality===2,'resuming repaired copy reuses each paid receipt');
 revised.f.docs.set(revisedPath+'/data/ad_quality_v4',{score:85,pass:false,issues:['Repeated descriptions']});revised.f.docs.delete(revisedPath+'/data/ad_quality_v11');revised.f.docs.delete(revisedPath+'/data/ad_proofs_v11');revised.f.docs.get(revisedPath).phase='needs_attention';
 await revised.svc.editorAIRun({workspaceId:revised.id,jobId:revised.jobId});
 ok(revised.calls.responses===3&&revised.calls.images===1&&revised.calls.quality===3,'latest review refines copy once with the existing photograph');
 revised.f.docs.get(revisedPath).phase='needs_attention';await revised.svc.editorAIRun({workspaceId:revised.id,jobId:revised.jobId});
 ok(revised.calls.responses===3&&revised.calls.quality===3&&revised.f.docs.get(revisedPath+'/data/ad_quality_v4').score===85,'latest refinement caches its receipt and preserves the former score');
 const interruptedCopy=await setup(),interruptedPath=interruptedCopy.p+'/editorAIJobs/'+interruptedCopy.jobId;
 interruptedCopy.f.docs.set(interruptedPath+'/data/ad_quality_v2',{score:83,pass:false,issues:['Copy needs work']});
 const responseBeforeInterrupt=interruptedCopy.D.responses;let repairDispatched=false;
 interruptedCopy.D.responses=async args=>{if(args.text?.format?.name==='brites_subject_focus')return responseBeforeInterrupt(args);if(!repairDispatched){repairDispatched=true;return responseBeforeInterrupt(args);}interruptedCopy.calls.responses++;throw Error('copy response outcome unknown');};
 await interruptedCopy.svc.editorAIRun({workspaceId:interruptedCopy.id,jobId:interruptedCopy.jobId});
 const interruptedStatus=await interruptedCopy.svc.editorAIStatus({...interruptedCopy.input,jobId:interruptedCopy.jobId});ok(!interruptedStatus.canRetry&&interruptedStatus.phase==='needs_attention','uncertain paid copy revision blocks automatic replacement');
 await assert.rejects(()=>interruptedCopy.svc.editorAIResume({...interruptedCopy.input,jobId:interruptedCopy.jobId}),/paid request/);n++;
 interruptedCopy.f.docs.set(interruptedPath+'/data/copy_refine_v3_response',{requestId:'original-repair-request',response:{model:'gpt-6-astra',output_text:JSON.stringify(revisedPlan),estimatedUsd:.2,costEstimated:false}});
 await interruptedCopy.svc.editorAIResume({...interruptedCopy.input,jobId:interruptedCopy.jobId});await interruptedCopy.svc.editorAIRun({workspaceId:interruptedCopy.id,jobId:interruptedCopy.jobId});
 ok(interruptedCopy.calls.responses===2&&(await interruptedCopy.svc.editorAIStatus({...interruptedCopy.input,jobId:interruptedCopy.jobId})).phase==='ready','recovered copy receipt resumes without another paid response');
 const interruptedFocus=await setup(),focusPath=interruptedFocus.p+'/editorAIJobs/'+interruptedFocus.jobId,normalResponses=interruptedFocus.D.responses;
 interruptedFocus.D.responses=async args=>{if(args.text?.format?.name==='brites_subject_focus'){interruptedFocus.calls.focus=(interruptedFocus.calls.focus||0)+1;throw Error('focus response outcome unknown');}return normalResponses(args);};
 await interruptedFocus.svc.editorAIRun({workspaceId:interruptedFocus.id,jobId:interruptedFocus.jobId});
 const focusFlight=interruptedFocus.f.docs.get(focusPath).inFlight;
 ok(focusFlight.key.startsWith('subject_focus_')&&!(await interruptedFocus.svc.editorAIStatus({...interruptedFocus.input,jobId:interruptedFocus.jobId})).canRetry,'an uncertain charm-location charge is not automatically repeated');
 await assert.rejects(()=>interruptedFocus.svc.editorAIResume({...interruptedFocus.input,jobId:interruptedFocus.jobId}),/paid request/);n++;
 interruptedFocus.f.docs.set(focusPath+'/data/'+focusFlight.key+'_response',{requestId:focusFlight.requestId,response:{model:'gpt-6-astra',output_text:JSON.stringify({...({landscape:{x:.58,y:.08,width:.29,height:.8},square:{x:.3,y:.04,width:.4,height:.38},portrait:{x:.28,y:.04,width:.44,height:.5},tall:{x:.29,y:.18,width:.42,height:.42},banner:{x:.04,y:.08,width:.23,height:.84},slim:{x:.29,y:.18,width:.42,height:.42}}[plan.scenePlans?.[e.calls.focus-1]?.key]||{x:.46,y:.65,width:.08,height:.18}),confident:true}),estimatedUsd:.03,costEstimated:false}});
 await interruptedFocus.svc.editorAIResume({...interruptedFocus.input,jobId:interruptedFocus.jobId});await interruptedFocus.svc.editorAIRun({workspaceId:interruptedFocus.id,jobId:interruptedFocus.jobId});
 ok(interruptedFocus.calls.focus===1&&(await interruptedFocus.svc.editorAIStatus({...interruptedFocus.input,jobId:interruptedFocus.jobId})).phase==='ready','a recovered charm-location receipt completes without another provider call');
 const waiting=await setup({autoProofs:false});await waiting.svc.editorAIRun({workspaceId:waiting.id,jobId:waiting.jobId});const pending=await waiting.svc.editorAIStatus({...waiting.input,jobId:waiting.jobId});ok(pending.phase==='awaiting_review'&&pending.candidate&&!pending.result&&waiting.calls.quality===0,'quality waits for actual rendered layouts before paying or allowing application');ok(waiting.f.docs.get(waiting.p+'/editorAIJobs/'+waiting.jobId).leaseUntil===0,'waiting for browser-rendered proofs releases the worker lock immediately');await assert.rejects(()=>waiting.svc.editorAIApply({...waiting.input,jobId:waiting.jobId}),/not ready/);n++;
 await assert.rejects(()=>waiting.svc.editorAIResume({...waiting.input,jobId:waiting.jobId,candidateHash:'wrong',reviewProofs:[]}),/changed before review/);n++;
 await assert.rejects(()=>waiting.svc.editorAIResume({...waiting.input,jobId:waiting.jobId,candidateHash:pending.candidate.candidateHash,reviewProofs:[]}),/complete bounded/);n++;
 const pinned=await setup({generatedSource:true}),pinnedRequest=pinned.f.docs.get(pinned.p+'/editorAIJobs/'+pinned.jobId+'/data/request');ok(pinnedRequest.sources[0].source.kind==='library'&&pinnedRequest.identitySources[0].source.kind==='product','new jobs retain artwork context but bind identity to original listing photos');const expectedBytes=await pinned.D.loadAsset(pinnedRequest.identitySources[0].asset),generate=pinned.D.generateImage;pinned.D.generateImage=async args=>{ok(args.references[0].equals(expectedBytes),'generation uses exact pinned product pixels rather than generated artwork');return generate(args);};pinned.D.reviewImages=async(source,files,brief)=>{ok(source.equals(expectedBytes)&&brief.reviewType==='complete_ad','complete-ad review uses the original product authority');return {pass:false,productFaithful:false,mobileReadable:true,score:94,issues:['Ring changed','x'.repeat(900)+' complete finding'],estimatedUsd:.1,costEstimated:false};};await pinned.svc.editorAIRun({workspaceId:pinned.id,jobId:pinned.jobId});const failedQuality=await pinned.svc.editorAIStatus({...pinned.input,jobId:pinned.jobId});ok(failedQuality.phase==='ready'&&failedQuality.result.needsRevision&&failedQuality.quality.score===94&&failedQuality.quality.issues[1].endsWith('complete finding'),'rejected review opens as a draft and retains exact score and findings');await pinned.svc.editorAIApply({...pinned.input,jobId:pinned.jobId});ok(pinned.f.docs.get(pinned.p).placements.length===6,'review score does not block applying generated assets');
 const priorCalls=JSON.stringify(pinned.calls),gallery=await pinned.svc.editorAIStatus({...pinned.input,jobId:pinned.jobId,includeReview:true});ok(gallery.candidate&&gallery.reviewProofs.length===27&&gallery.reviewProofs.every(p=>p.url),'failed reviewed artwork and signed proofs can be inspected without application');ok(JSON.stringify(pinned.calls)===priorCalls,'viewing proofs makes no paid generation or review request');
 const galleryPath=pinned.p+'/editorAIJobs/'+pinned.jobId+'/data/',reviewedProofs=pinned.f.docs.get(galleryPath+'ad_proofs_v11');pinned.f.docs.set(galleryPath+'ad_quality_v3',pinned.f.docs.get(galleryPath+'ad_quality_v11'));pinned.f.docs.set(galleryPath+'ad_proofs_v3',reviewedProofs);pinned.f.docs.delete(galleryPath+'ad_quality_v11');pinned.f.docs.set(galleryPath+'ad_proofs_v11',{candidateHash:'unreviewed',images:[]});
 ok((await pinned.svc.editorAIStatus({...pinned.input,jobId:pinned.jobId,includeReview:true})).reviewProofs.length===27,'gallery stays bound to the scored proof version while a new review is pending');
 // New aspect-aware plans retain independent receipts and deterministic family routing.
 const oldPlans=plan.scenePlans;
 for(const count of [5,6]){
  plan.scenePlans=engine.sceneCatalog.slice(0,count).map(s=>({key:s.key,reason:'Distinct crop for '+s.key,direction:{...plan.imageDirections[0],composition:s.direction}}));
  const multi=await setup(),formats=[],gen=multi.D.generateImage;multi.D.generateImage=async args=>{formats.push(args.format.requestSize);return gen(args);};
  let resumed=false;
  if(count===6){const realNow=Date.now,started=realNow();multi.D.generateImage=async args=>{const out=await gen(args);formats.push(args.format.requestSize);Date.now=()=>started+5*60000;return out;};try{const first=await multi.svc.editorAIRun({workspaceId:multi.id,jobId:multi.jobId});ok(first.continue&&multi.calls.images===1,'long scene run yields after a saved receipt');ok(multi.f.docs.get(multi.p+'/editorAIJobs/'+multi.jobId).leaseUntil===0,'continuation releases worker lease');}finally{Date.now=realNow;multi.D.generateImage=async args=>{formats.push(args.format.requestSize);return gen(args);};}resumed=true;}
  await multi.svc.editorAIRun({workspaceId:multi.id,jobId:multi.jobId});
  const done=await multi.svc.editorAIStatus({...multi.input,jobId:multi.jobId});assert.equal(done.phase,'ready',done.error);n++;
  ok(multi.calls.images===count&&multi.calls.focus===count,'every '+count+'-scene source is generated and localized once');
  assert.deepEqual(formats,engine.sceneCatalog.slice(0,count).map(s=>s.format.requestSize));n++;
  for(const b of engine.boards){const im=engine.selectImage(plan,done.result.responsive.images,b);ok(im.forBoards?.includes(b.key)||im.forFamilies.includes(engine.family(b)),b.key+' gets its assigned scene');}
  await multi.svc.editorAIRun({workspaceId:multi.id,jobId:multi.jobId});ok(multi.calls.images===count,'completed multi-scene retry buys no replacements');
 }
 plan.scenePlans=plan.scenePlans.slice(0,5);
 for(const fixed of [true,false]){
  const repair=await setup(),response=repair.D.responses;repair.D.responses=async args=>{const out=await response(args);if(args.text?.format?.name==='brites_subject_focus'&&[2,6].includes(repair.calls.focus))out.output_text=JSON.stringify({x:.3,y:.04,width:.4,height:repair.calls.focus===6&&fixed?.38:.5,confident:true});return out;};
  const first=await repair.svc.editorAIRun({workspaceId:repair.id,jobId:repair.jobId});ok(first.continue&&repair.calls.images===6,'only the unsuitable square scene receives one bounded correction');
  await repair.svc.editorAIRun({workspaceId:repair.id,jobId:repair.jobId});const state=await repair.svc.editorAIStatus({...repair.input,jobId:repair.jobId});
  ok(repair.calls.images===6,'correction resumes without regenerating the other scenes');
  if(fixed)ok(state.phase==='ready'&&repair.calls.quality===1,'corrected framing reaches complete-ad review');else {ok(state.phase==='ready'&&repair.calls.quality===1,'a safe retained layout reaches review without a repeated hard stop');ok([...repair.f.docs.entries()].some(([key,value])=>key.endsWith('/data/scene_fit_notes')&&value.retainedLayouts.length),'retained framing limitations remain recorded');}
 }
 const budget=await setup();budget.D.control=async()=>({creativeBudgetUsd:budget.calls.responses?1:30});budget.D.reserveCost=async()=>({reservedUsd:.6});await budget.svc.editorAIRun({workspaceId:budget.id,jobId:budget.jobId});const budgetStatus=await budget.svc.editorAIStatus({...budget.input,jobId:budget.jobId});ok(budgetStatus.phase==='ready','former spending cap does not stop generation');ok(budget.calls.images>=5,'all scenes complete despite former spending cap');
 // The operator's framing is the product evidence. What they cropped is what the
 // AI must see, for the static scenes and the films that reuse the same reference.
 async function referenceSeenBy(options){
  const e=await setup(options),seen={};
  const gen=e.D.generateImage;e.D.generateImage=async args=>{seen.generate=args.references[0];return gen(args);};
  e.D.reviewImages=async(src,files,brief)=>{seen.review=src;e.calls.quality++;return {pass:true,productFaithful:true,mobileReadable:true,score:92,issues:[],estimatedUsd:.1,costEstimated:false};};
  await e.svc.editorAIRun({workspaceId:e.id,jobId:e.jobId});
  const state=await e.svc.editorAIStatus({...e.input,jobId:e.jobId});
  assert.equal(state.phase,'ready',state.error);n++;
  const job=e.f.docs.get(e.p+'/editorAIJobs/'+e.jobId+'/data/request');
  return {e,seen,identity:job.identitySources||[],meta:await sharp(seen.generate).metadata(),pixel:(await sharp(seen.generate).raw().toBuffer({resolveWithObject:true})).data};
 }
 const uncropped=await referenceSeenBy({});
 ok(uncropped.meta.width===1024&&uncropped.meta.height===1024,'an untouched listing photo is referenced whole');
 // Cropping the photo on the artboard must trim the reference to the same frame.
 const onCanvas=await referenceSeenBy({layer:{cropX:200,cropY:300,width:600,height:500}});
 ok(onCanvas.meta.width===600&&onCanvas.meta.height===500,'an artboard crop reaches the AI as the cropped frame, not the full original');
 ok(onCanvas.identity[0].framedFrom&&onCanvas.identity[0].frame.x===200&&onCanvas.identity[0].frame.y===300,'the pinned identity records the exact operator frame');
 ok(onCanvas.seen.review.equals(onCanvas.seen.generate),'the quality review judges the same cropped reference the scene was generated from');
 // A saved crop chosen from the repository is that listing photo, trimmed on purpose.
 const savedCrop=await referenceSeenBy({makeSource:async(e,original)=>{
  const cropped=await sharp({create:{width:800,height:600,channels:3,background:'#ff0000'}}).jpeg().toBuffer();
  const asset=await e.D.saveAsset(e.id,cropped,'operator_crop',{width:800,height:600,mimeType:'image/jpeg'});
  e.f.docs.set(e.p+'/imageLibrary/crop_operator',{id:'crop_operator',kind:'crop',groupRef:plan.groupRef,format:'square',title:'Cropped listing photo',productIds:[plan.productId],rootSource:{kind:'product',productId:plan.productId,imageId:'img1'},artwork:false,asset,createdAt:Date.now()});
  return e.svc.editorSource({workspaceId:e.id,productId:plan.productId,groupRef:plan.groupRef,source:{kind:'library',imageId:'crop_operator'}});
 },layer:{width:800,height:600}});
 ok(savedCrop.meta.width===800&&savedCrop.meta.height===600,'a saved operator crop is referenced at its own size');
 ok(savedCrop.pixel[0]>240&&savedCrop.pixel[1]<20,'the AI receives the cropped pixels, never the untrimmed listing photo');
 ok(savedCrop.identity.length===1&&savedCrop.identity[0].source.kind==='library','the crop itself is the pinned identity instead of falling back to the original');
 // Generated artwork still cannot pose as product identity.
 const artwork=await setup({makeSource:async(e,original)=>{
  const asset=await e.D.saveAsset(e.id,await sharp({create:{width:900,height:900,channels:3,background:'#00ff00'}}).jpeg().toBuffer(),'prior_art',{width:900,height:900,mimeType:'image/jpeg'});
  e.f.docs.set(e.p+'/imageLibrary/crop_artwork',{id:'crop_artwork',kind:'crop',groupRef:plan.groupRef,format:'square',title:'Saved design',productIds:[plan.productId],rootSource:{kind:'savedDesign',imageId:'d1'},artwork:true,asset,createdAt:Date.now()});
  return e.svc.editorSource({workspaceId:e.id,productId:plan.productId,groupRef:plan.groupRef,source:{kind:'library',imageId:'crop_artwork'}});
 },layer:{width:900,height:900}});
 const artworkRequest=artwork.f.docs.get(artwork.p+'/editorAIJobs/'+artwork.jobId+'/data/request');
 ok(artworkRequest.identitySources.length===1&&artworkRequest.identitySources[0].source.kind==='product','a saved design crop never becomes the product identity');

 // Targeted fixes: one reviewed deduction, one bounded correction, every other paid scene reused.
 const reviewWith=(calls)=>async()=>{calls.quality++;return {pass:false,productFaithful:true,mobileReadable:true,score:88,scores:{messaging:90,layout:80,relevance:100,visualAppeal:100,productRecognition:100},categoryReviews:{messaging:{summary:'Hook is generic',deductions:[{points:10,reason:'Generic hook',evidence:'300×250 headline',correction:'Rewrite the headline as a specific gift-occasion hook',kind:'required',formats:[]}]},layout:{summary:'Backdrop busy',deductions:[{points:20,reason:'Busy backdrop competes with the charm',evidence:'desktop_display_300x250 photograph background',correction:'Simplify the photographed backdrop behind the charm',kind:'required',formats:['desktop_display_300x250']}]},relevance:{summary:'ok',deductions:[]},visualAppeal:{summary:'ok',deductions:[]},productRecognition:{summary:'ok',deductions:[]}},issues:['Busy backdrop'],estimatedUsd:.1,costEstimated:false};};
 async function finishFix(e,jobId){let st;for(let i=0;i<4;i++){await e.svc.editorAIRun({workspaceId:e.id,jobId});st=await e.svc.editorAIStatus({...e.input,jobId,allSizes:true});if(st.phase!=='queued')break;}if(st.phase==='awaiting_review'){const c=st.candidate,boards=[{...c.artboard,key:'active'},...engine.variants.map(b=>({...b,key:b.device+'_'+b.key}))],proofs=[];for(const b of boards){const scale=Math.min(1,960/Math.max(b.width,b.height));proofs.push({key:b.key,width:b.width,height:b.height,renderCheck:{version:1,visiblePhotoFraction:.3},dataBase64:(await sharp({create:{width:Math.round(b.width*scale),height:Math.round(b.height*scale),channels:3,background:'#d1b284'}}).jpeg().toBuffer()).toString('base64')});}await e.svc.editorAIResume({...e.input,jobId,candidateHash:c.candidateHash,reviewProofs:proofs});await e.svc.editorAIRun({workspaceId:e.id,jobId});st=await e.svc.editorAIStatus({...e.input,jobId,allSizes:true});}return st;}
 const fixable=await setup();fixable.D.reviewImages=reviewWith(fixable.calls);await fixable.svc.editorAIRun({workspaceId:fixable.id,jobId:fixable.jobId});
 const reviewed=await fixable.svc.editorAIStatus({...fixable.input,jobId:fixable.jobId,allSizes:true});
 ok(reviewed.phase==='ready'&&reviewed.canFix&&reviewed.fixOptions.length===2,'a reviewed design lists one targeted fix per deduction without stopping');
 const planFix=reviewed.fixOptions.find(o=>o.category==='messaging'),sceneFix=reviewed.fixOptions.find(o=>o.category==='layout');
 ok(planFix.kind==='plan'&&sceneFix.kind==='scene'&&sceneFix.sceneKey==='square'&&sceneFix.formats.join()==='desktop_display_300x250'&&planFix.formats.join()==='desktop_display_300x250','messaging revises the plan; the backdrop fix targets only the square scene');
 await assert.rejects(()=>fixable.svc.editorAIFix({...fixable.input,jobId:fixable.jobId,fix:{category:'layout',index:4}}),/no longer available/);n++;
 await assert.rejects(()=>fixable.svc.editorAIFix({...fixable.input,jobId:fixable.jobId,reviewHash:'stale',fix:{category:'layout',index:0}}),/review changed/);n++;
 const beforeScene={...fixable.calls},sf=await fixable.svc.editorAIFix({...fixable.input,jobId:fixable.jobId,fix:{category:'layout',index:0}});
 ok(sf.queued&&sf.fix.kind==='scene'&&(await fixable.svc.editorAIFix({...fixable.input,jobId:fixable.jobId,fix:{category:'layout',index:0}})).jobId===sf.jobId,'scene fix is durable and idempotent');
 ok(fixable.f.docs.get(fixable.p).editorAI.id===sf.jobId&&fixable.f.docs.get(fixable.p+'/editorAIJobs/'+sf.jobId+'/data/scene_landscape').reusedFrom===fixable.jobId&&!fixable.f.docs.has(fixable.p+'/editorAIJobs/'+sf.jobId+'/data/scene_square'),'the fix reuses every other saved scene and drops only the targeted one');
 const generatedDirections=[],gen=fixable.D.generateImage;fixable.D.generateImage=async args=>{generatedDirections.push(args.imageDirections[0].composition);return gen(args);};
 const sceneDone=await finishFix(fixable,sf.jobId);
 ok(sceneDone.phase==='ready'&&sceneDone.fixOf===fixable.jobId&&sceneDone.fixTarget.kind==='scene',sceneDone.error||'scene fix completes and links to the reviewed design');
 ok([1,2].includes(fixable.calls.images-beforeScene.images)&&[1,2].includes(fixable.calls.focus-beforeScene.focus)&&fixable.calls.responses===beforeScene.responses&&fixable.calls.quality===beforeScene.quality+1,'scene fix buys only the targeted scene (plus its one bounded fit correction), one localization and one review');
 ok(generatedDirections.length&&generatedDirections.every(d=>d.includes('Simplify the photographed backdrop')),'every regenerated scene image receives the reviewed correction');
 ok(sceneDone.result.publicationImages.length===3&&sceneDone.cost.estimatedUsd<1,'fix result is complete and priced by its own stages only');
 const beforePlan={...fixable.calls},pf=await fixable.svc.editorAIFix({...fixable.input,jobId:fixable.jobId,fix:{category:'messaging',index:0}});
 let planPrompt='';const resp=fixable.D.responses;fixable.D.responses=async args=>{if(args.text?.format?.name!=='brites_subject_focus')planPrompt=args.input[0].content;return resp(args);};
 const planDone=await finishFix(fixable,pf.jobId);
 ok(planDone.phase==='ready'&&planDone.fixTarget.kind==='plan',planDone.error||'plan fix completes');
 ok(fixable.calls.images===beforePlan.images&&fixable.calls.focus===beforePlan.focus&&fixable.calls.responses===beforePlan.responses+1&&fixable.calls.quality===beforePlan.quality+1,'plan fix buys one text revision and one review, no images');
 ok(planPrompt.includes('targeted correction of ONE reviewed finding'),'plan revision is scoped to the single finding');
 await fixable.svc.editorAIApply({...fixable.input,jobId:pf.jobId});ok(fixable.f.docs.get(fixable.p).placements.length===6,'a corrected design applies to the workspace like any reviewed design');
 if(oldPlans===undefined)delete plan.scenePlans;else plan.scenePlans=oldPlans;
 console.log('PASS '+n+' responsive scene generation, native application, idempotency and paid-recovery checks');
})().catch(e=>{console.error(e.stack);process.exitCode=1});
