const assert=require('node:assert/strict'),sharp=require('sharp'),fs=require('node:fs'),{JSDOM}=require('jsdom');
const refs=require('../../netlify/functions/googleAdsMotionReferences'),mode=require('../../netlify/functions/googleAdsMotionInput'),locks=require('../../netlify/functions/googleAdsMotionProductLock');
const {createMotionService,motionPrompt}=require('../../netlify/functions/googleAdsAdMotion');
const clone=v=>JSON.parse(JSON.stringify(v));let checks=0;const check=(v,m)=>{assert(v,m);checks++;};
function memory(){const docs=new Map(),doc=p=>({id:p.split('/').pop(),path:p,get:async()=>({exists:docs.has(p),data:()=>clone(docs.get(p)||null)}),set:async v=>docs.set(p,clone(v)),update:async v=>docs.set(p,{...docs.get(p),...clone(v)}),collection:n=>collection(p+'/'+n)}),collection=p=>({doc:n=>doc(p+'/'+n),get:async()=>({docs:[...docs].filter(([k])=>k.startsWith(p+'/')&&!k.slice(p.length+1).includes('/')).map(([k,v])=>({id:k.split('/').pop(),data:()=>clone(v)}))})});return {docs,db:{collection,runTransaction:fn=>fn({get:r=>r.get(),set:(r,v)=>r.set(v),update:(r,v)=>r.update(v),delete:r=>docs.delete(r.path)})}};}
const direction=()=>refs.validate({geometry:Object.fromEntries(refs.GEOMETRY.map(k=>[k,'OBSERVED_'+k])),supportingReferenceIndices:[1],setting:'Replace the scene with a mountain vista.',middle:'Add moving scenery.'},2);
const good=()=>({pass:true,productFaithful:true,exactProductIdentity:true,mobileReadable:true,footageLettering:false,multipleProducts:false,score:98,issues:[],formatIdentity:Object.fromEntries(refs.KEYS.map(k=>[k,{sameOutline:true,noAddedDetail:true,noMissingDetail:true,sameFeatures:true}]))});
const response=v=>({status:'completed',output_text:JSON.stringify(v),estimatedUsd:.01});
(async()=>{
 const sourceList=[{id:'catalog',asset:{path:'catalog'},productId:'p'},{id:'scene',asset:{path:'scene'},productId:'p'},{id:'logo',asset:{path:'logo'},productId:'p'}];
 const doc={objects:[{type:'Image',sourceKey:'logo',editorRole:'brand'},{type:'Image',sourceKey:'scene',editorRole:'shape',width:1,height:1},{type:'Image',sourceKey:'scene',editorRole:'photo'},{type:'Textbox',text:'Marketing copy'}]};
 check(mode.designPhoto(doc,sourceList,'p').id==='scene','the photo layer of the primary preview chooses its clean scene even when the catalog photo is listed first');
 check(!mode.designPhoto(doc,sourceList,'other'),'another product cannot supply the design photo');
 check(!mode.designPhoto({objects:doc.objects.slice(0,2)},sourceList,'p'),'brand and background layers cannot become the product photo');
 const designStore=memory(),designScope={workspaceId:'design_package',productId:'p',groupRef:'g'},workspaceRef=designStore.db.collection('State').doc('adDesign').collection('workspaces').doc(designScope.workspaceId);
 await workspaceRef.set({settings:designScope,context:{campaignId:'42'}});
 const scopeHash=require('node:crypto').createHash('sha256').update(JSON.stringify(['campaign:42','g','p'])).digest('hex').slice(0,40),savedDesigns=designStore.db.collection('State').doc('adDesignSaved').collection('scopes').doc(scopeHash).collection('designs');
 for(const s of sourceList)await workspaceRef.collection('editorSources').doc(s.id).set({...s,groupRef:'g',width:64,height:64});
 const selected={id:'selected',workspaceId:designScope.workspaceId,productId:'p',groupRef:'g',createdAt:1,document:doc,sourceIds:sourceList.map(s=>s.id)};
 await savedDesigns.doc('selected').set(selected);await savedDesigns.doc('newest').set({...selected,id:'newest',createdAt:2,document:{objects:[{type:'Image',sourceKey:'catalog',editorRole:'photo'}]}});
 const designs=require('../../netlify/functions/googleAdsAdDesign').createAdDesignService({fb:()=>designStore,COL:{state:'State'},env:{},loadAsset:async()=>{throw Error('An uncropped photo should not be rewritten');}});
 const chosen=await designs.editorMotionBasis({...designScope,savedDesignId:'selected',firstFrame:true});
 check(chosen.asset.path==='scene'&&chosen.designId==='selected','the selected package wins over a newer unrelated design and its catalog source');
 check((await designs.editorMotionFirstFrame(designScope)).designId==='newest','without an explicit selection the first saved preview supplies the photo');
 await savedDesigns.doc('selected').update({deletedAt:1});await assert.rejects(designs.editorMotionFirstFrame({...designScope,savedDesignId:'selected'}),/unavailable/);checks++;
 await savedDesigns.doc('selected').set({...selected,document:{objects:[{type:'Image',sourceKey:'missing',editorRole:'photo'}]}});await assert.rejects(designs.editorMotionFirstFrame({...designScope,savedDesignId:'selected'}),/no saved clean photo/);checks++;
 const editorId='eai_'+'b'.repeat(40),editorRef=workspaceRef.collection('editorAIJobs').doc(editorId);await editorRef.set({phase:'ready',scope:designScope});await editorRef.collection('data').doc('result').set({document:doc,sources:sourceList});
 check((await designs.editorMotionFirstFrame({...designScope,fromEditorWorker:true,editorJobId:editorId})).asset.path==='scene','automatic static-plus-video uses its own completed primary scene, not a different saved design');
 const f=memory(),ref=f.db.collection('Workspaces').doc('design_test'),scope={workspaceId:'design_test',productId:'p',groupRef:'g'},id='motion_'+'a'.repeat(40),target=ref.collection('motionJobs').doc(id),blobs=new Map(),calls={video:[],planning:0};
 const designPhoto=await sharp({create:{width:96,height:96,channels:3,background:'#f1e1b5'}}).png().toBuffer();
 const photos=await Promise.all(['#c9a369','#c2b790'].map(background=>sharp({create:{width:64,height:64,channels:3,background}}).png().toBuffer()));
 await ref.set({settings:scope,context:{groups:[{ref:'g'}]}});
 const parent={id,...scope,title:'Generic jewelry',phase:'queued',pipelineVersion:6,renderVersion:14,motionMode:refs.MODE,referencePolicy:refs.POLICY,createdAt:1,updatedAt:1,leaseUntil:0,inFlight:null,masters:{},variants:[],quality:null,creativeDirection:direction(),originalSources:photos.map((_,i)=>({source:{kind:'product',productId:'p'},productId:'p',asset:{path:'original'+i}})),plan:{copy:{headline:'Everyday favourite',shortHeadline:'Your jewelry',description:'Discover the collection.',cta:'Shop now'},nativeCopy:{}},composition:Object.fromEntries(['portrait','square','landscape'].map(k=>[k,{x:.4,y:.4,w:.3,h:.4}]))};
 await target.set(parent);
 const D={fb:()=>f,context:async()=>({ref,w:(await ref.get()).data(),products:[]}),firstFrameFor:async()=>({designId:'saved_selected',sourceId:'scene_first',productId:'p',groupRef:'g',asset:{path:'design-photo'},selection:'design-primary-photo'}),loadAsset:async a=>a.path==='design-photo'?designPhoto:photos[Number(a.path.at(-1))],loadVideo:async a=>blobs.get(a.path),saveVideo:async(w,b,key,info)=>{blobs.set(key,b);return {path:key,hash:refs.hash(b),...info};},signVideo:async a=>'https://example.test/'+a.path,
  planMotion:async req=>{calls.planning++;if(req.text.format.name==='brites_photo_product_lock')return response({geometry:direction().geometry,shapePlan:{contour:'PHOTO_BOUND_CONTOUR',features:[{name:'Photo boundary feature',count:2,positions:'PHOTO_BOUND_POSITION',appearance:'PHOTO_BOUND_SHAPE'}],confusionRisks:'Follow these exact photographed openings'},motion:{support:'Supported exactly as shown',flexibleMotion:'Only the visible flexible hardware',environmentMotion:'Only the visible fabric',camera:'A short translation near the original viewing angle'}});return response(req.text.format.name==='brites_referenced_motion'?{...direction(),shapePlan:{contour:'Original contour',features:[{name:'Original feature',count:1,positions:'Original position',appearance:'Original shape'}],confusionRisks:'Follow the photograph'},sceneStory:{motif:'Abstract',connection:'The photographed materials complement the jewelry.',propMotion:'The photographed fabric moves.'}}:Object.fromEntries(['portrait','square','landscape'].map(k=>[k,{bounds:[.4,.45,.25,.4],complete:true,confidence:1,note:'Complete item'}])));},sampleMotionFrames:async()=>[{orientation:'landscape',second:1,bytes:photos[0]}],
  videoRequest:async(route,method,body)=>{calls.video.push(clone(body));return {id:'v1_film_'+calls.video.length,status:'completed',steps:[{type:'model_output',content:[{type:'video',data:Buffer.from('film'+calls.video.length).toString('base64')}]}]};},videoContent:async data=>Buffer.from(data.data,'base64'),
  renderVariants:async(b,o)=>[{key:o==='landscape'?'desktop_landscape':'mobile_'+o,device:o==='landscape'?'desktop':'mobile',format:o,width:720,height:720,seconds:10,bytes:Buffer.from('export_'+o+'_'+b.toString()),frames:Array.from({length:6},()=>photos[0])}],reviewImages:async()=>good()};
 const service=createMotionService(D);
 check((await service.status(scope)).nextGenerationMode===mode.PHOTO,'new requests default to the photo-first trial');
 check((await service.run({...scope,jobId:id})).ok,'a historical job still runs');
 check(calls.video.every(b=>b.generation_config.video_config.task===mode.REFERENCE&&b.input.filter(i=>i.type==='image').length===2),'deploying the trial cannot change old queued jobs or their reference inputs');
 const before=clone((await target.get()).data());
 const start=await service.start({...scope,redoOf:id,redoFormat:'portrait',confirmRedo:true}),newRef=ref.collection('motionJobs').doc(start.jobId);
 check((await newRef.get()).data().generationMode===mode.PHOTO,'redo pins the new generation mode');
 // Switch the preference while the queued film exists; that film remains photo-first.
 const saved=await service.start({...scope,saveGenerationMode:true,generationMode:mode.REFERENCE});
 check(saved.queued===false&&calls.video.length===3,'saving a method cannot buy a film');
 check((await createMotionService(D).status(scope)).nextGenerationMode===mode.REFERENCE,'the choice survives a fresh service instance');
 check((await service.run({...scope,jobId:start.jobId})).ok,'photo-first redo completes');
 const body=calls.video.at(-1),prompt=body.input.at(-1).text,fresh=(await newRef.get()).data();
 check(body.generation_config.video_config.task===mode.PHOTO&&body.input.filter(i=>i.type==='image').length===1,'one original is sent with explicit image-to-video mode');
 check(body.input[0].mime_type==='image/png'&&Buffer.from(body.input[0].data,'base64').equals(designPhoto),'the selected design photo, not the catalog reference, reaches the provider unchanged');
 check(prompt.includes('FIRST FRAME')&&prompt.includes('real physical motion')&&!prompt.includes('mountain vista')&&prompt.includes('PHOTO_BOUND_CONTOUR')&&prompt.includes('OBSERVED_negativeSpace'),'the new prompt anchors the photo, carries its observed identity and excludes old scenery');
 check(prompt.includes(refs.REPLICATION_RULES)&&prompt.includes('extending only the existing background'),'identity has priority while aspect changes extend the backdrop');
 check(prompt.includes(refs.CONTINUITY_RULES)&&!prompt.includes('For the first 1–2 seconds')&&prompt.includes('THROUGHOUT 0–10 SECONDS'),'outbound generation requires persistent identity throughout the middle, not only the opening');
 check(calls.video.length===4&&calls.planning===2,'redo prepares one photo-bound identity and one layout, then buys only its one film');
 for(const o of ['square','landscape']){check(JSON.stringify(fresh.masters[o])===JSON.stringify(before.masters[o]),'other '+o+' master remains untouched');const key=o==='landscape'?'desktop_landscape':'mobile_square';check(JSON.stringify(fresh.variants.find(v=>v.key===key))===JSON.stringify(before.variants.find(v=>v.key===key)),'other '+o+' export remains untouched');}
 check(JSON.stringify((await target.get()).data())===JSON.stringify(before),'the original version is unchanged');
 const receipt=(await newRef.collection('receipts').doc('video_request_portrait').get()).data();
 check(receipt.task===mode.PHOTO&&receipt.references.length===1&&receipt.references[0].source.path==='design-photo'&&receipt.references[0].designId==='saved_selected'&&receipt.prompt===prompt&&fresh.masters.portrait.promptRevision==='design-photo-product-fidelity-continuous-v3'&&receipt.productLockHash===fresh.productLock.hash,'receipt records the actual first-frame request');
 check(locks.matches(fresh.productLock,fresh.firstFrameHash)&&fresh.productLock.firstFrameHash===fresh.firstFrameHash,'identity observations are bound to the exact selected photograph');
 const tampered=clone(fresh.productLock);tampered.shapePlan.features[0].count++;check(!locks.matches(tampered,fresh.firstFrameHash),'changed identity observations cannot reuse the original lock receipt');
 assert.throws(()=>motionPrompt({...fresh,firstFrameHash:'f'.repeat(64)},'portrait'),/exact design photo/);checks++;
 const state=await service.status(scope);check(state.generationMode===mode.PHOTO&&state.nextGenerationMode===mode.REFERENCE,'status distinguishes the saved film method from the next request');
 const undo=await service.start({...scope,redoOf:start.jobId,redoFormat:'portrait',confirmRedo:true});
 check((await service.run({...scope,jobId:undo.jobId})).ok,'switching back completes normally');
 check(calls.video.at(-1).generation_config.video_config.task===mode.REFERENCE&&calls.video.at(-1).input.filter(i=>i.type==='image').length===2,'undo restores the previous multi-reference provider path');
 const paid=calls.video.length;await service.run({...scope,jobId:undo.jobId});check(calls.video.length===paid,'resuming never duplicates a paid film');
 // A completely fresh set prepares from the chosen design scene too.
 const originalContext=D.context;D.context=async()=>({...await originalContext(),products:[{id:'p',title:'Generic jewelry',url:'https://example.test/jewelry'}]});
 D.motionBasis=async()=>({design:{id:'basis'},sources:parent.originalSources,originalSources:parent.originalSources});
 const oldPlanner=D.planMotion;D.planMotion=async req=>{if(req.text.format.name==='brites_photo_product_lock'){const images=req.input[1].content.filter(c=>c.type==='input_image');check(images.length===1&&images[0].image_url==='data:image/png;base64,'+designPhoto.toString('base64'),'fresh planning studies the exact design scene used by the video');}return oldPlanner(req);};
 const full=await service.start({...scope,generationMode:mode.PHOTO});check((await service.run({...scope,jobId:full.jobId})).ok,'a fresh three-format design-photo set completes');
 check(calls.video.slice(-3).every(b=>b.generation_config.video_config.task===mode.PHOTO&&Buffer.from(b.input[0].data,'base64').equals(designPhoto)),'every format in the new set starts from the same saved design photo');
 const fullJob=(await ref.collection('motionJobs').doc(full.jobId).get()).data();
 check(['portrait','square','landscape'].every(o=>fullJob.masters[o].productLockHash===fullJob.productLock.hash),'all formats consume the same photo-bound identity record');
 check(calls.video.slice(-3).every(b=>b.input.at(-1).text.includes(refs.CONTINUITY_RULES)),'all three actual provider requests require the same continuous shot');
 const lockCalls=calls.planning,videoCalls=calls.video.length;await service.run({...scope,jobId:full.jobId});check(calls.planning===lockCalls&&calls.video.length===videoCalls,'a completed resume purchases neither a lock nor a replacement film');
 const again=await service.start({...scope,redoOf:full.jobId,redoFormat:'portrait',confirmRedo:true,generationMode:mode.PHOTO});
 check((await service.run({...scope,jobId:again.jobId})).ok,'one-format fidelity redo completes');
 const againJob=(await ref.collection('motionJobs').doc(again.jobId).get()).data();check(againJob.productLock.hash===fullJob.productLock.hash&&calls.planning===lockCalls+1,'same-photo redo reuses the identity preparation and only plans its changed layout');
 // Another selected package must prepare identity again from its own photo.
 const alternatePhoto=await sharp({create:{width:64,height:64,channels:3,background:'#ddeeff'}}).png().toBuffer(),originalLoader=D.loadAsset;
 D.firstFrameFor=async()=>({designId:'different_package',sourceId:'different_scene',productId:'p',groupRef:'g',asset:{path:'different-photo'},selection:'design-primary-photo'});
 D.loadAsset=async asset=>asset.path==='different-photo'?alternatePhoto:originalLoader(asset);
 D.planMotion=async req=>{if(req.text.format.name==='brites_photo_product_lock')check(req.input[1].content.find(c=>c.type==='input_image').image_url==='data:image/png;base64,'+alternatePhoto.toString('base64'),'changed-package preparation studies the new photo');return oldPlanner(req);};
 const changed=await service.start({...scope,redoOf:again.jobId,redoFormat:'square',confirmRedo:true,generationMode:mode.PHOTO,savedDesignId:'different_package'});
 const changedResult=await service.run({...scope,jobId:changed.jobId});check(changedResult.ok,'a redo from another selected package completes: '+JSON.stringify(changedResult));
 const changedJob=(await ref.collection('motionJobs').doc(changed.jobId).get()).data();check(changedJob.productLock.firstFrameHash!==againJob.productLock.firstFrameHash&&changedJob.productLock.hash!==againJob.productLock.hash,'another package cannot inherit the first photo identity record');
 check(Buffer.from(calls.video.at(-1).input[0].data,'base64').equals(alternatePhoto),'the new package photo reaches the video request without masking or replacement');
 // Incomplete preparation is corrected once and then stops before any video purchase.
 const brokenStore=memory(),brokenRef=brokenStore.db.collection('State').doc('broken'),brokenId='motion_'+ 'b'.repeat(40);await brokenRef.set({settings:scope});
 await brokenRef.collection('motionJobs').doc(brokenId).set({...parent,id:brokenId,generationMode:mode.PHOTO,photoFidelityPolicy:mode.PRODUCT_POLICY,firstFrameSource:{asset:{path:'design-photo'},selection:'design-primary-photo',productId:'p'},creativeDirection:null});
 let brokenPlans=0,brokenFilms=0;
 const brokenService=createMotionService({...D,fb:()=>brokenStore,context:async()=>({ref:brokenRef,w:(await brokenRef.get()).data()}),planMotion:async()=>{brokenPlans++;return response({geometry:direction().geometry,motion:{support:'Not visible',flexibleMotion:'None visible',environmentMotion:'None visible',camera:'Near-view translation'}});},videoRequest:async()=>{brokenFilms++;throw Error('Must not purchase a film without the photo identity');}});
 check(!(await brokenService.run({...scope,jobId:brokenId})).ok&&brokenPlans===2&&brokenFilms===0,'an incomplete photo identity never dispatches a film or loops through paid retries');
 await assert.rejects(service.start({...scope,saveGenerationMode:true,generationMode:'bogus'}),/Choose/);checks++;
 await assert.rejects(service.start({...scope,productId:'other',saveGenerationMode:true,generationMode:mode.PHOTO}),/another product/);checks++;
 for(const format of ['portrait','square','landscape']){const p=motionPrompt({...parent,generationMode:mode.PHOTO},format);check(p.includes('FIRST FRAME')&&!p.includes('mountain vista'),'first-frame instructions apply to '+format);}
 // Identity inventories remain generic and all formats receive the exact prepared observations.
 for(const [name,count,hardware] of [['celestial pendant',1,'visible attachment only'],['animal pendant',1,'visible loop only'],['irregular charm',1,'no chain'],['earring pair',2,'two supplied fittings'],['object charm',1,'no chain'],['ring',1,'no pendant fitting']]){
  const observations={geometry:{...direction().geometry,jewelryType:name,assembly:String(count)+' supplied item(s)',attachment:hardware},shapePlan:{contour:name+' observed outline',features:[{name:'observed boundary group',count,positions:'As photographed',appearance:'As photographed'}],confusionRisks:'Never substitute the usual form of '+name},motion:{support:'Original support',flexibleMotion:'Only present flexible parts',environmentMotion:'Only visible surroundings',camera:'Near-view translation'}};
  const firstFrameHash='a'.repeat(64),lock=locks.bind(observations,firstFrameHash);
  for(const format of ['portrait','square','landscape']){const p=motionPrompt({...parent,generationMode:mode.PHOTO,photoFidelityPolicy:mode.PRODUCT_POLICY,firstFrameHash,productLock:lock},format);check(p.includes(name+' observed outline')&&p.includes(hardware)&&p.includes('"count":'+count),'generic '+name+' identity reaches '+format);}
 }
 // A paid, cached motion plan may have been written before continuity was
 // strengthened. Replace its unsafe actions without changing its identity.
 const continuityHash='c'.repeat(64),cached=locks.bind({geometry:direction().geometry,shapePlan:{contour:'Original outline',features:[{name:'Original opening',count:1,positions:'As photographed',appearance:'As photographed'}],confusionRisks:'Preserve the original opening'},motion:{support:'The charm disappears behind the support.',flexibleMotion:'The chain covers the charm.',environmentMotion:'Dissolve to a new scene.',camera:'Rack focus to the foreground and reveal a replacement charm.'}},continuityHash),cachedBefore=JSON.stringify(cached);
 for(const format of ['portrait','square','landscape']){const p=motionPrompt({...parent,generationMode:mode.PHOTO,photoFidelityPolicy:mode.PRODUCT_POLICY,firstFrameHash:continuityHash,productLock:cached},format);check(Object.values(cached.motion).every(v=>!p.includes(v))&&p.includes('Original outline')&&p.includes(refs.CONTINUITY_RULES),'cached conflicting actions are removed before '+format+' dispatch');}
 check(JSON.stringify(cached)===cachedBefore&&locks.matches(cached,continuityHash),'sanitizing unsafe motion leaves the paid identity record and binding unchanged');
 check(locks.request(parent,[{bytes:designPhoto,mimeType:'image/png'}]).input[0].content.includes(refs.CONTINUITY_RULES),'photo preparation plans product permanence before generation');
 // Browser interactions: persistence, failed save, chosen method on a one-film redo, and unchanged ratio control.
 const dom=new JSDOM('<div id="host"></div>',{runScripts:'outside-only',url:'https://example.test'}),w=dom.window;w.setTimeout=()=>0;w.clearTimeout=()=>{};w.eval(fs.readFileSync('brites-ad-motion.js','utf8'));
 let preference=mode.PHOTO,fail=false,sent=[];const uiState={...state,jobId:'motion_saved',phase:'ready',nextGenerationMode:preference,redoOptions:[{format:'portrait',orientation:'portrait',formats:['mobile_portrait'],kept:['mobile_square','desktop_landscape'],hasFilm:true,estimatedUsd:1.01}]};
 const request=async(a,b)=>{if(a==='adDesignMotionStatus')return {...uiState,nextGenerationMode:preference};sent.push(b);if(b.saveGenerationMode){if(fail)throw Error('Save failed');preference=b.generationMode;return {ok:true,nextGenerationMode:preference,queued:false};}return {ok:true,queued:true,jobId:'motion_new',making:['portrait']};};
 w.BritesAdMotion.mount(w.document.getElementById('host'),{scope,design:{id:'saved_selected',savedPreview:true},request});await new Promise(setImmediate);const q=s=>w.document.querySelector(s),picker=q('[data-generation-mode]');
 check(picker.value===mode.PHOTO,'trial mode is visible in the panel');picker.value=mode.REFERENCE;await picker.onchange({target:picker});check(preference===mode.REFERENCE&&sent.length===1&&sent[0].saveGenerationMode,'one selection saves the previous method without generation');
 fail=true;picker.value=mode.PHOTO;await picker.onchange({target:picker});check(picker.value===mode.REFERENCE&&q('[data-mode-status]').textContent==='Save failed','failed saves restore the last confirmed choice and show the error');fail=false;
 q('[data-redo]').click();check(q('[data-confirm]').textContent.includes('Create a new scene'),'confirmation states the selected method');await q('[data-confirm] .bam-primary').onclick();check(sent.at(-1).generationMode===mode.REFERENCE&&sent.at(-1).redoFormat==='portrait'&&sent.at(-1).savedDesignId==='saved_selected','redo sends the selected previous method explicitly');
 dom.window.close();
 console.log('PASS '+checks+' photo-first generation, reversible preference, paid-job preservation and UI checks');require('./suite-guard.cjs').done();
})().catch(e=>{console.error(e.stack);process.exitCode=1;});
