// Saved ad sets: every size is checked where the charm really is in each saved
// photo (free pixel measurement, no AI request), and an on-demand fix remakes
// only the photos that need it. Offline: fake providers and in-memory storage.
const fs=require('node:fs'),vm=require('node:vm'),path=require('node:path'),crypto=require('node:crypto'),assert=require('node:assert/strict'),sharp=require('sharp');
const fixture=path.join(__dirname,'ad-design-workflow.cjs'),source=fs.readFileSync(fixture,'utf8').split('(async()=>{const e=await setup();')[0];
const ctx=vm.createContext({require:require('node:module').createRequire(fixture),__dirname,process,Buffer,console,Date,setTimeout,clearTimeout});vm.runInContext(source+'\nglobalThis.setupFixture=setup;',ctx);
const root=path.resolve(__dirname,'../..'),responsive=require(root+'/brites-ad-responsive'),{plan:basePlan}=require('./ad-responsive.cjs');
const sha=v=>crypto.createHash('sha256').update(typeof v==='string'?v:JSON.stringify(v)).digest('hex'),clone=v=>JSON.parse(JSON.stringify(v));
let n=0;const ok=(v,m)=>{assert(v,m);n++;};
const productId=basePlan.productId,groupRef=basePlan.groupRef,BASE=['landscape','square','portrait','tall','banner','slim'];
// The two specialised shapes, for sets saved with them (a saved set carries its own board routing).
const MID={midLandscape:{format:{key:'landscape',width:1536,height:1056},families:[],boards:['display_580x400']},midPortrait:{format:{key:'portrait',width:1296,height:2048},families:[],boards:['display_240x400','display_250x360']}};
// Where the charm really is in each photo. The saved AI box adds a small margin, as a real one does.
const CHARM={landscape:{x:.62,y:.2,width:.22,height:.6},square:{x:.4,y:.08,width:.2,height:.28},portrait:{x:.36,y:.1,width:.28,height:.3},tall:{x:.36,y:.24,width:.28,height:.2},banner:{x:.08,y:.2,width:.16,height:.6},slim:{x:.36,y:.2,width:.28,height:.2},midLandscape:{x:.6,y:.16,width:.24,height:.5},midPortrait:{x:.36,y:.1,width:.28,height:.26}};
const grow=b=>({x:b.x-.04,y:b.y-.04,width:b.width+.08,height:b.height+.08});
const photos=new Map();
async function photo(w,h,b){
 const key=JSON.stringify([w,h,b]);if(photos.has(key))return photos.get(key);
 const cx=(b.x+b.width/2)*w,cy=(b.y+b.height/2)*h,rx=b.width*w/2,ry=b.height*h/2;
 const svg=`<svg xmlns="http://www.w3.org/2000/svg" width="${w}" height="${h}"><rect width="100%" height="100%" fill="#efe6d6"/><line x1="${cx}" y1="0" x2="${cx}" y2="${cy-ry}" stroke="#b08d3c" stroke-width="3"/><ellipse cx="${cx}" cy="${cy}" rx="${rx}" ry="${ry}" fill="#c9a23f"/><ellipse cx="${cx-rx*.3}" cy="${cy-ry*.2}" rx="${rx*.25}" ry="${ry*.2}" fill="#8a6a1f"/></svg>`;
 const bytes=await sharp(Buffer.from(svg)).jpeg({quality:90}).toBuffer();photos.set(key,bytes);return bytes;
}
async function setup(){
 const e=await ctx.setupFixture();e.loads=0;const load=e.D.loadAsset;e.D.loadAsset=async a=>{e.loads++;return load(a);};
 // Distinct planning reservations per stage, so the estimate is checked exactly.
 e.quotes=[];e.D.reserveCost=async q=>{e.quotes.push(q);return {reservedUsd:q.key==='quality'?.2:q.key==='subject_focus'?.05:.4};};
 return e;
}
// A design saved before this change: a ready job, its frozen layouts, review and proofs, written as the old run left them.
async function savedSet(e,{name,scenes=BASE,charm={},aiBox={},phase='ready',legacy=false}={}){
 const w=e.f.docs.get(e.p),jobId='eai_'+sha(['saved',name]).slice(0,40),jp=e.p+'/editorAIJobs/'+jobId,device='shared',artboard={key:'square',width:2048,height:2048};
 const plan={...clone(basePlan),style:{...basePlan.style,treatment:'soft-fade'},...(legacy?{}:{scenePlans:scenes.map(k=>({key:k,reason:'A dedicated '+k+' photograph',direction:{...basePlan.imageDirections[0]}}))})};
 const images=[],sources=[],data={};
 for(const key of legacy?['0']:scenes){
  const spec=legacy?{format:{width:2048,height:2048},families:[],boards:[]}:responsive.sceneCatalog.find(s=>s.key===key)||MID[key],b=charm[key]||CHARM[legacy?'square':key],bytes=await photo(spec.format.width,spec.format.height,b);
  const asset=await e.D.saveAsset(e.id,bytes,jobId+'_scene_'+key,{width:spec.format.width,height:spec.format.height,mimeType:'image/jpeg'}),id='scene_'+sha([e.id,jobId,'scene_'+key]).slice(0,40);
  sources.push({id,productId,groupRef,title:'Duck necklace · '+key+' scene',width:asset.width,height:asset.height,asset,source:{kind:'library',imageId:'generated_'+sha(asset.path).slice(0,32)}});
  images.push({id,width:asset.width,height:asset.height,focalX:.5,focalY:.5,forFamilies:spec.families,forBoards:spec.boards,...(legacy?{}:{sceneKey:key}),focus:aiBox[key]||grow(b)});
  data['scene_'+key]={asset,requestId:'paid_'+key,reservedUsd:.4,receivedAt:1};data['subject_focus_'+sha(asset).slice(0,24)]={focus:aiBox[key]||grow(b),requestId:'focus_'+key};
 }
 const docFor=b=>responsive.document(plan,responsive.selectImage(plan,images,b),b,b.device);
 const candidate={document:responsive.document(plan,responsive.selectImage(plan,images,artboard),artboard,'mobile'),productId,groupRef,device,artboard,destination:'https://britesjewelry.com/products/duck',sources,publicationImages:[],nativeCopy:plan.nativeCopy,
  responsive:{layoutVersion:responsive.layoutVersion,plan,images,boards:responsive.boards,variants:responsive.variants,documents:responsive.variants.map(b=>({key:b.key,device:b.device,width:b.width,height:b.height,document:docFor(b)}))},alternatives:[],rationale:plan.rationale,sourceIds:plan.sourceIds,evidenceHash:'facts',limitations:[]};
 const candidateHash=sha(candidate),proof=await e.D.saveAsset(e.id,await sharp({create:{width:64,height:64,channels:3,background:'#d1b284'}}).jpeg().toBuffer(),jobId+'_proof',{width:64,height:64,mimeType:'image/jpeg'});
 const proofImages=[{...artboard,key:'active'},...responsive.variants.map(b=>({...b,key:b.device+'_'+b.key}))].map(b=>({key:b.key,width:b.width,height:b.height,displayWidth:b.width,displayHeight:b.height,renderCheck:{version:1,visiblePhotoFraction:.3},asset:proof})),proofs={candidateHash,images:proofImages,proofHash:sha(proofImages),createdAt:1};
 const review={score:93,pass:true,productFaithful:true,mobileReadable:true,scores:{messaging:95,layout:90,relevance:95,visualAppeal:92,productRecognition:95},categoryReviews:Object.fromEntries(['messaging','layout','relevance','visualAppeal','productRecognition'].map(k=>[k,{summary:'Good',deductions:[]}])),issues:[],candidateHash,proofHash:proofs.proofHash};
 const identity=await e.D.saveAsset(e.id,await photo(1024,1024,CHARM.square),jobId+'_identity',{width:1024,height:1024,mimeType:'image/jpeg'});
 Object.assign(data,{request:{productId,groupRef,device,artboard,document:{objects:[]},mode:'design',responsive:true,includeAnimation:false,selectedLayerId:'',instruction:'',identitySources:[{id:'listing',asset:identity}],sources:[{id:'listing',asset:identity}],publicationBaseline:'saved'},
  response:{response:{model:'claude-sonnet-5-5',output_text:JSON.stringify(plan)},requestId:'plan'},evidence:{hash:'facts',researchCompletedAt:1,sources:[],warnings:[]},candidate:{...candidate,candidateHash},crop_square:{id:'generated_crop',asset:proof},
  ...(phase==='ready'?{result:{...candidate,quality:review,needsRevision:false,candidateHash,proofHash:proofs.proofHash},ad_proofs_v11:proofs,ad_quality_v11:review}:{})});
 const now=Date.now();e.f.docs.set(jp,{id:jobId,requestId:'saved_'+name,inputHash:sha(name),designKey:'design_'+sha([productId,groupRef,device,artboard.key,artboard.width,artboard.height]).slice(0,32),scope:{productId,groupRef,device,artboard},sourceSetId:w.sourceSetId,sourceVersion:w.sourceVersion||null,snapshotHash:w.snapshotHash||null,phase,owner:null,leaseUntil:0,createdAt:now,updatedAt:now,progress:{pct:phase==='ready'?100:92,label:'Saved'},inFlight:null,reservedUsd:0,stageUsage:[]});
 for(const [key,value] of Object.entries(data))e.f.docs.set(jp+'/data/'+key,clone(value));
 return {jobId,jp,input:{workspaceId:e.id,productId,groupRef,device,artboard,jobId},candidate,candidateHash,images,plan};
}
const failing=st=>st.placement.results.filter(r=>!r.ok).map(r=>r.key);
const PORTRAIT_FAMILY=['mobile_portrait','desktop_portrait','desktop_display_240x400','desktop_display_250x360'];
(async()=>{
 // 1. The Duck Charm case: the portrait photo is fine, only its saved AI box sits low.
 const e=await setup(),low=await savedSet(e,{name:'low-box',aiBox:{portrait:{...grow(CHARM.portrait),y:grow(CHARM.portrait).y+.14}}}),candidateBefore=clone(e.f.docs.get(low.jp+'/data/candidate'));
 const st=await e.svc.editorAIStatus({...low.input,allSizes:true,includeReview:true});
 ok(st.ok&&st.placement&&st.placement.version===require(root+'/brites-ad-placement').VERSION&&st.placement.ok===false&&Number.isFinite(st.placement.checkedAt),'a saved set gets a placement check in its status');
 ok(st.placement.results.length===1+responsive.variants.length&&st.placement.results[0].key==='active'&&st.placement.results.every(r=>typeof r.ok==='boolean'&&Array.isArray(r.issues)&&Array.isArray(r.warnings)),'every size is listed by its key, active first');
 assert.deepEqual(failing(st),PORTRAIT_FAMILY);n++;
 ok(PORTRAIT_FAMILY.every(k=>st.placement.results.find(r=>r.key===k).issues.some(i=>i.kind==='cut'&&i.edges.includes('top'))),'each portrait-family size cuts the charm at the top, as Paul saw');
 ok(e.loads===6,'each saved photo is loaded once for the measurement');
 const fix=st.placementFix;
 assert.deepEqual(fix.sceneKeys,['midLandscape','midPortrait']);n++;
 assert.deepEqual(fix.relayoutFormats,['mobile_portrait','desktop_portrait']);n++;
 ok(!fix.sceneKeys.includes('portrait'),'a photo that is fine is not bought again: its sizes are only laid out again');
 ok(fix.available===true&&fix.reason===null&&['desktop_display_580x400','desktop_display_240x400','desktop_display_250x360','mobile_portrait','desktop_portrait'].every(k=>fix.formats.includes(k)),'the fix lists every size it changes');
 ok(fix.estimatedUsd===1.1&&fix.label==='Reframe 2 sizes and make 2 new photos · about US$1.10','two new photos, their charm localization and one review are priced; reframing is free');
 ok(/240x400 and 250x360/.test(fix.corrections.midPortrait)&&/580x400/.test(fix.corrections.midLandscape)&&Object.values(fix.corrections).every(t=>t.includes('Keep the complete charm and bail inside the photo with a clear margin.')&&!/desktop_|mobile_|display_/.test(t)),'corrections are plain words per new photo');
 ok(e.calls.responses===0&&e.calls.images===0&&e.calls.quality===0,'checking a saved set makes no paid request');
 assert.deepEqual(e.f.docs.get(low.jp+'/data/candidate'),candidateBefore);n++;
 ok(e.f.docs.get(low.jp+'/data/placement_v1')?.candidateHash===low.candidateHash,'the check is cached for this exact set');
 const again=await e.svc.editorAIStatus({...low.input,allSizes:true});
 ok(e.loads===6&&again.placement.checkedAt===st.placement.checkedAt&&JSON.stringify(again.placementFix)===JSON.stringify(fix),'a second status call reuses the cached check without loading the photos');
 const plain=await e.svc.editorAIStatus(low.input);ok(plain.placement===null&&plain.placementFix===null&&e.loads===6,'a plain progress poll does not check placement');
 const engineVersion=responsive.layoutVersion;responsive.layoutVersion=engineVersion+1;let bumped;try{bumped=await e.svc.editorAIStatus({...low.input,allSizes:true});}finally{responsive.layoutVersion=engineVersion;}
 ok(e.loads===6&&e.f.docs.get(low.jp+'/data/placement_v1').layoutVersion===engineVersion+1&&JSON.stringify(bumped.placementFix)===JSON.stringify(fix),'a new layout engine re-derives the check from the saved measurements without reloading photos');
 ok(st.fixTarget===null&&st.canFix===false,'review fixes are unchanged');

 // 2. A photo that itself cuts the charm must be made again.
 const cut=await savedSet(e,{name:'cut-photo',charm:{portrait:{x:.36,y:-.06,width:.28,height:.3}},aiBox:{portrait:{x:.32,y:0,width:.36,height:.3}}});e.quotes.length=0;
 const cs=await e.svc.editorAIStatus({...cut.input,allSizes:true});
 ok(PORTRAIT_FAMILY.every(k=>cs.placement.results.find(r=>r.key===k).issues.some(i=>i.kind==='source-cut'&&i.edges.includes('top'))),'sizes using a photo that cuts the charm are listed');
 assert.deepEqual(cs.placementFix.sceneKeys,['portrait','midLandscape','midPortrait']);n++;
 ok(!cs.placementFix.relayoutFormats.length&&/cut off the charm at the top/.test(cs.placementFix.corrections.portrait)&&/portrait on phones and portrait on desktop/.test(cs.placementFix.corrections.portrait),'the remade photo is told which sizes and edges failed');
 ok(cs.placementFix.estimatedUsd===1.55&&/^Make 3 new photos for 5 sizes · about US\$1\.55$/.test(cs.placementFix.label),'the label says what is made and what it costs');
 const priced=e.quotes.slice(-7);
 ok(priced.map(q=>q.key.replace(/^image_.*/,'image')).join()==='image,subject_focus,image,subject_focus,image,subject_focus,quality'&&priced[0].format.requestSize===responsive.sceneCatalog.find(s=>s.key==='portrait').format.requestSize&&priced[4].format.height>priced[4].format.width&&priced.every(q=>q.workspace&&q.job.inputCoverage.preparedReferenceCount===1),'each new photo is priced at its own shape with its localization, then one review, as the run reserves them');

 // 3. Fixing a saved set on demand: never automatic, never trusting the browser's scene list.
 const noImage=clone(e.D.env);delete e.D.env.OPENAI_API_KEY;await assert.rejects(()=>e.svc.editorAIFix({...cut.input,fix:{kind:'placement'}}),/OPENAI_API_KEY is missing/);n++;e.D.env.OPENAI_API_KEY=noImage.OPENAI_API_KEY;
 delete e.D.env.ANTHROPIC_API_KEY;await assert.rejects(()=>e.svc.editorAIFix({...cut.input,fix:{kind:'placement'}}),/ANTHROPIC_API_KEY is missing/);n++;e.D.env.ANTHROPIC_API_KEY=noImage.ANTHROPIC_API_KEY;
 const pressed=await e.svc.editorAIFix({...cut.input,fix:{kind:'placement',sceneKeys:['square'],category:'layout',index:0}}),childId='eai_'+sha([cut.jobId,'placement',['portrait','midLandscape','midPortrait']]).slice(0,40);
 ok(pressed.ok&&pressed.queued&&pressed.jobId===childId,'the fix job has a stable id from the design and its scenes');
 assert.deepEqual(pressed.fix.sceneKeys,['portrait','midLandscape','midPortrait']);n++;
 const child=e.f.docs.get(e.p+'/editorAIJobs/'+childId),childData=k=>e.f.docs.get(e.p+'/editorAIJobs/'+childId+'/data/'+k);
 ok(child.phase==='queued'&&child.fixOf===cut.jobId&&child.fix.kind==='placement'&&child.fix.parentReviewVersion===11&&child.fix.estimatedUsd===1.55&&child.fix.label===cs.placementFix.label&&JSON.stringify(child.fix.corrections)===JSON.stringify(cs.placementFix.corrections),'the child job carries the whole fix');
 ok(child.progress.label==='Saved · new photos will be made for 5 sizes','progress says what will happen in plain words');
 ok(JSON.stringify(childData('request').fix)===JSON.stringify(child.fix)&&childData('request').includeAnimation===false,'the request carries the same fix');
 ok(['scene_landscape','scene_square','scene_tall','scene_banner','scene_slim','response','evidence'].every(k=>childData(k)?.reusedFrom===cut.jobId)&&Object.keys(childData('scene_landscape')).includes('asset'),'every other paid photo and receipt is reused');
 ok(!childData('scene_portrait')&&['candidate','result','ad_proofs_v11','ad_quality_v11','crop_square','placement_v1'].every(k=>!childData(k)),'only the listed photo is dropped, with the saved layouts, review and check');
 ok(e.f.docs.get(e.p).editorAI.id===childId&&e.calls.images===0&&e.calls.responses===0&&e.calls.quality===0,'pressing Fix only saves the job; the run makes the photos');
 const second=await e.svc.editorAIFix({...cut.input,fix:{kind:'placement'}});ok(second.jobId===childId&&second.queued===false,'a second press returns the same job');
 const childStatus=await e.svc.editorAIStatus({...cut.input,jobId:childId,allSizes:true});
 ok(childStatus.fixOf===cut.jobId&&childStatus.fixTarget.kind==='placement'&&childStatus.fixTarget.sceneKeys.join()==='portrait,midLandscape,midPortrait'&&childStatus.fixTarget.label===cs.placementFix.label&&childStatus.fixTarget.estimatedUsd===1.55,'the fix job status names its target');
 ok(childStatus.placement===null&&/not finished/.test(childStatus.placementFix.reason)&&childStatus.placementFix.available===false,'a set still being made has nothing to fix yet');
 ok(Object.keys(e.f.docs.get(cut.jp+'/data/scene_portrait')).includes('asset'),'the saved design keeps its own photos');

 // 4. A saved set with every photo shape and the charm clear everywhere.
 const clean=await savedSet(e,{name:'clean',scenes:[...BASE,'midLandscape','midPortrait']}),cl=await e.svc.editorAIStatus({...clean.input,includeReview:true});
 ok(cl.placement.ok&&!failing(cl).length&&cl.placementFix.available===false&&/^Nothing to fix/.test(cl.placementFix.reason)&&cl.placementFix.label===null&&cl.placementFix.estimatedUsd===0,'a clean complete set has nothing to fix');
 await assert.rejects(()=>e.svc.editorAIFix({...clean.input,fix:{kind:'placement'}}),/Nothing to fix/);n++;

 // 5. With both specialised photos saved, a low box only needs a new layout: no photo, no image key needed.
 const relay=await savedSet(e,{name:'relay',scenes:[...BASE,'midLandscape','midPortrait'],aiBox:{portrait:{...grow(CHARM.portrait),y:grow(CHARM.portrait).y+.14}}}),rs=await e.svc.editorAIStatus({...relay.input,allSizes:true});
 assert.deepEqual(failing(rs),['mobile_portrait','desktop_portrait']);n++;
 ok(!rs.placementFix.sceneKeys.length&&rs.placementFix.relayoutFormats.join()==='mobile_portrait,desktop_portrait'&&rs.placementFix.estimatedUsd===.2&&rs.placementFix.label==='Reframe 2 sizes · about US$0.20','a reframe alone costs only the complete-ad review');
 delete e.D.env.OPENAI_API_KEY;const reframed=await e.svc.editorAIFix({...relay.input,fix:{kind:'placement'}});e.D.env.OPENAI_API_KEY=noImage.OPENAI_API_KEY;
 const reframeJob=e.f.docs.get(e.p+'/editorAIJobs/'+reframed.jobId);
 ok(reframed.queued&&reframeJob.progress.label==='Saved · 2 sizes will be reframed'&&!reframeJob.fix.sceneKeys.length,'a reframe needs no new photos');
 ok([...BASE,'midLandscape','midPortrait'].every(k=>e.f.docs.get(e.p+'/editorAIJobs/'+reframed.jobId+'/data/scene_'+k)?.reusedFrom===relay.jobId),'a reframe reuses every saved photo');

 // 6. A size that still cuts the charm when laid out around the measured charm needs a new photo.
 const stuck=await savedSet(e,{name:'stuck',scenes:[...BASE,'midLandscape','midPortrait'],aiBox:{portrait:{...grow(CHARM.portrait),y:grow(CHARM.portrait).y+.14}}}),document=responsive.document,lowBox=stuck.images.find(i=>i.sceneKey==='portrait').focus;
 responsive.document=(plan,image,board,device)=>document(plan,image.sceneKey==='portrait'?{...image,focus:lowBox}:image,board,device);
 let ss;try{ss=await e.svc.editorAIStatus({...stuck.input,allSizes:true});}finally{responsive.document=document;}
 ok(ss.placementFix.sceneKeys.join()==='portrait'&&!ss.placementFix.relayoutFormats.length&&/^Make 1 new photo for 2 sizes · about US\$0\.65$/.test(ss.placementFix.label),'a layout that cannot hold the charm gets a new photo');

 // 7. Older designs, unfinished sets and photos that cannot be loaded never fail the status.
 const legacy=await savedSet(e,{name:'legacy',legacy:true}),ls=await e.svc.editorAIStatus({...legacy.input,allSizes:true});
 ok(ls.ok&&ls.placement===null&&ls.placementFix.available===false&&/older design/.test(ls.placementFix.reason),'an older design without AI photo sets is explained, not failed');
 const running=await savedSet(e,{name:'running',phase:'awaiting_review'}),rn=await e.svc.editorAIStatus({...running.input,includeReview:true});
 ok(rn.placement===null&&/not finished/.test(rn.placementFix.reason),'a set waiting for its review is not checked yet');
 const broken=await savedSet(e,{name:'broken'}),loader=e.D.loadAsset;e.D.loadAsset=async()=>{throw Error('storage offline');};
 let bs;try{bs=await e.svc.editorAIStatus({...broken.input,allSizes:true});}finally{e.D.loadAsset=loader;}
 ok(bs.ok&&bs.phase==='ready'&&bs.placement===null&&/could not be checked/.test(bs.placementFix.reason)&&!e.f.docs.get(broken.jp+'/data/placement_v1'),'a photo that cannot load leaves a plain reason and no cached result');
 ok((await e.svc.editorAIStatus({...broken.input,allSizes:true})).placement.ok===true,'the check runs again once the photos load');
 console.log('PASS '+n+' saved-set charm placement and on-demand fix checks');
 // Run after the checks above: they patch the layout engine for a moment.
 await browserChecks();
 require('./suite-guard.cjs').done();
})().catch(error=>{console.error(error.stack);process.exitCode=1;});

// The browser's own proof check travels with the proofs: sanitized, never a reason to refuse them, and merged into the status.
async function browserChecks(){
 let m=0;const good=(v,msg)=>{assert(v,msg);m++;};
 const e=await setup(),set=await savedSet(e,{name:'browser',scenes:[...BASE,'midLandscape','midPortrait'],phase:'awaiting_review'}),bytes=await photo(64,64,CHARM.square);
 const boards=[{...set.candidate.artboard,key:'active'},...responsive.variants.map(b=>({...b,key:b.device+'_'+b.key}))],proofs=[];
 for(const b of boards){const exact=/^display_/.test(b.key.replace(/^(mobile|desktop)_/,''))&&b.width<=1050,scale=exact?1:Math.min(1,960/Math.max(b.width,b.height));proofs.push({key:b.key,width:b.width,height:b.height,renderCheck:{version:1,visiblePhotoFraction:.3},dataBase64:(await sharp(bytes).resize(Math.round(b.width*scale),Math.round(b.height*scale),{fit:'fill'}).jpeg().toBuffer()).toString('base64')});}
 const at=k=>proofs.find(p=>p.key===k);
 at('desktop_display_300x250').placement={version:1,ok:false,issues:[{kind:'covered',layers:['ai_headline',7,'x'.repeat(400)],message:'x'.repeat(500),extra:'dropped'},{kind:'bogus',message:'no'},{kind:'cut',edges:['top','sideways','top'],message:'Charm cut off at the top.'}],extra:true};
 at('desktop_display_336x280').placement={version:1,ok:false,issues:[{kind:'covered',layers:['ai_brand'],message:'A text or logo layer covers the charm.'}]};
 at('mobile_square').placement='not a check';at('desktop_square').placement={version:1,ok:false,issues:Array.from({length:14},()=>({kind:'missing',message:'No product photo is in this size.'}))};
 const resumed=await e.svc.editorAIResume({...set.input,candidateHash:set.candidateHash,reviewProofs:proofs});
 good(resumed.ok,'proofs with browser checks are accepted');
 const saved=e.f.docs.get(set.jp+'/data/ad_proofs_v11'),placed=k=>saved.images.find(i=>i.key===k).placement;
 good(JSON.stringify(placed('desktop_display_300x250'))===JSON.stringify({version:1,ok:false,issues:[{kind:'covered',layers:['ai_headline','x'.repeat(200)],message:'x'.repeat(200)},{kind:'cut',edges:['top'],message:'Charm cut off at the top.'}]}),'only known kinds, edges and bounded text are kept');
 good(placed('mobile_square')===undefined&&placed('desktop_square').issues.length===10&&placed('active')===undefined,'a malformed check is ignored and long lists are bounded');
 // The run finishes the set as before; the status merges the browser check over the declared boxes.
 const proof=saved,result={...set.candidate,quality:{score:93,pass:true},needsRevision:false,candidateHash:set.candidateHash,proofHash:proof.proofHash};
 e.f.docs.set(set.jp+'/data/result',result);e.f.docs.set(set.jp+'/data/ad_quality_v11',{score:93,pass:true,productFaithful:true,mobileReadable:true,categoryReviews:{},issues:[],candidateHash:set.candidateHash,proofHash:proof.proofHash});Object.assign(e.f.docs.get(set.jp),{phase:'ready',leaseUntil:0});
 const st=await e.svc.editorAIStatus({...set.input,allSizes:true}),row=k=>st.placement.results.find(r=>r.key===k);
 good(!row('desktop_display_300x250').ok&&row('desktop_display_300x250').rendered===true&&row('desktop_display_300x250').issues.map(i=>i.kind).join()==='covered,cut','a size the browser saw failing counts, with its exact text finding');
 good(!row('desktop_display_336x280').ok&&row('mobile_square').ok&&row('active').ok,'sizes without a browser finding keep the declared-box result');
 good(st.placementFix.available&&st.placementFix.relayoutFormats.includes('desktop_display_300x250')&&!st.placementFix.sceneKeys.length,'a browser cut is reframed when the measured charm fits the new layout');
 good(!st.placementFix.relayoutFormats.includes('desktop_display_336x280')&&!st.placementFix.formats.includes('desktop_display_336x280'),'text over the charm alone is not fixed by new photos');
 // With only the text finding left, the fix explains what to do instead.
 const only=await setup(),set2=await savedSet(only,{name:'covered',scenes:[...BASE,'midLandscape','midPortrait']}),proofDoc=only.f.docs.get(set2.jp+'/data/ad_proofs_v11');
 proofDoc.images.find(i=>i.key==='desktop_display_336x280').placement={version:1,ok:false,issues:[{kind:'covered',layers:['ai_brand'],message:'A text or logo layer covers the charm.'}]};
 const cv=await only.svc.editorAIStatus({...set2.input,allSizes:true});
 good(!cv.placement.ok&&cv.placementFix.available===false&&cv.placementFix.reason==='Text or the logo covers the charm in 336x280. New photos will not fix that; move the text in the editor.','text over the charm gets a plain instruction instead of a paid fix');
 good(e.calls.images+e.calls.responses+e.calls.quality+only.calls.images+only.calls.responses+only.calls.quality===0,'no paid request anywhere');
 console.log('PASS '+m+' browser proof check storage and merge checks');
}
