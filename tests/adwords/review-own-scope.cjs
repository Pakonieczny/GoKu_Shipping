// One design workspace holds four complete-ad reviews (Duck, Gecko, Bunny, Saturn) while its current selection is
// Duck. Every review reads, edits, prepares, checks and publishes its own product scope: its own images, films,
// display proofs and plan, never the selected product's. Reviews are frozen saved packages: later studio edits do not
// make them stale. Reviews saved before frozen packages (no sourceMode or sourceSetId) still read their own scope and
// convert to a saved package. A job in flight for Duck never blocks another product's review.
// Offline: Firestore, storage, Google Ads and the worker are fakes; nothing is generated, uploaded or sent.
const assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm'),path=require('node:path'),crypto=require('node:crypto');
const dir=path.resolve(__dirname,'../../netlify/functions')+'/',file=dir+'googleAdsAutopilot.js',realRequire=require('node:module').createRequire(file),sharp=realRequire('sharp');
const D=realRequire('./googleAdsAdDesign'),filmReviewHash=realRequire('./googleAdsMotionPublication').reviewHash,refs=realRequire('./googleAdsMotionReferences');
const clone=x=>x==null?x:JSON.parse(JSON.stringify(x)),sha=v=>crypto.createHash('sha256').update(typeof v==='string'?v:JSON.stringify(v)).digest('hex');
let checks=0;const ok=(v,m)=>{assert(v,m);checks++;};
const net=[];
function engine(){const context=vm.createContext({module:{exports:{}},exports:{},require:n=>n==='node-fetch'?async url=>{net.push(String(url));throw Error('Live network forbidden: '+url);}:realRequire(n),process:{env:{GADS_CUSTOMER_ID:'123'}},console,Buffer,Date,Intl,Map,Set,URL,setTimeout,clearTimeout});vm.runInContext(fs.readFileSync(file,'utf8'),context);return {E:context.module.exports,get:n=>vm.runInContext(n,context),bind(values){context.__m=values;vm.runInContext(Object.keys(values).map(k=>k+'=__m.'+k).join('\n'),context);}};}
function memory(){const docs=new Map(),files=new Map();
 const doc=p=>({id:p.split('/').pop(),path:p,get:async()=>({exists:docs.has(p),id:p.split('/').pop(),data:()=>clone(docs.get(p))}),set:async v=>{docs.set(p,clone(v));},update:async v=>{if(!docs.has(p))throw Error('Missing document '+p);const next=clone(docs.get(p));for(const [key,value] of Object.entries(clone(v))){const parts=key.split('.');let at=next;for(const part of parts.slice(0,-1))at=at[part]||(at[part]={});at[parts.at(-1)]=value;}docs.set(p,next);},delete:async()=>{docs.delete(p);},collection:n=>collection(p+'/'+n)});
 const collection=p=>({doc:n=>doc(p+'/'+n),where(){return this;},select(){return this;},limit(){return this;},get:async()=>({docs:[...docs.keys()].filter(k=>k.startsWith(p+'/')&&!k.slice(p.length+1).includes('/')).map(k=>({id:k.split('/').pop(),data:()=>clone(docs.get(k))}))})});
 return {docs,files,db:{collection,runTransaction:async fn=>fn({get:r=>r.get(),set:(r,v)=>r.set(v),update:(r,v)=>r.update(v),delete:r=>r.delete()})},FV:{serverTimestamp:()=>Date.now()},admin:{storage:()=>({bucket:()=>({file:p=>({download:async()=>{if(!files.has(p))throw Error('Saved file missing '+p);return [files.get(p)];},getSignedUrl:async()=>['https://storage.test/'+p]})})})}};}

const WS='design_animals',GROUP='opportunity:animals',WSP='Brites_GAds_State/adDesign/workspaces/'+WS,FORMATS=[['square',600,600],['landscape',1200,628],['portrait',600,750]],KEYS=['mobile_portrait','mobile_square','desktop_landscape'];
const P={duck:{n:101,title:'Duck Silhouette Charm Necklace',color:'#e8c34a'},gecko:{n:102,title:'Gecko Necklace',color:'#5aa04b'},bunny:{n:103,title:'Bunny Pendant Necklace',color:'#d9a7b0'},saturn:{n:104,title:'Planet Saturn Pendant Necklace',color:'#7a6fb0'}};
for(const [name,p] of Object.entries(P))Object.assign(p,{name,id:'gid://shopify/Product/'+p.n,offer:'shopify_us_'+p.n+'_1',url:'https://britesjewelry.com/products/'+name+'-necklace',motionJob:'motion_'+sha(name).slice(0,40)});
const cap=s=>s[0].toUpperCase()+s.slice(1),copyFor=name=>({headlines:[cap(name)+' Charm','A Little '+cap(name)+' Necklace','Gift a '+cap(name)+' Pendant'],longHeadlines:['Discover the '+name+' necklace at Brites Jewelry'],descriptions:['Shop the '+name+' necklace at Brites Jewelry.','Find a '+name+' charm for your collection today.']});
const settingsOf=name=>({productId:P[name].id,groupRef:GROUP,sourceImageId:'img_'+name,selectedImages:[{productId:P[name].id,imageId:'img_'+name}],referenceIds:[],currentAssetIds:[],direction:'',style:{background:'product-led'},deviceLinked:true,formats:['square','landscape','portrait']});
const filmHash=(name,key)=>sha(Buffer.from('film '+name+' '+key).toString('base64'));
const film=name=>({id:P[name].motionJob,workspaceId:WS,productId:P[name].id,groupRef:GROUP,title:P[name].title,destination:P[name].url,phase:'ready',pipelineVersion:2,plan:{copy:{headline:cap(name)},nativeCopy:copyFor(name)},quality:{pass:true,productFaithful:true,mobileReadable:true,score:98},variants:KEYS.map(key=>({key,format:key.split('_')[1],seconds:10,asset:{path:'Brites_GAds_Motion/'+P[name].motionJob+'/'+key+'.mp4',hash:filmHash(name,key)}})),createdAt:1000+P[name].n});
// A film set as motion-publication.cjs makes it publishable: generated from the product's original references.
const publishable=job=>{job={...job,motionMode:refs.MODE,referencePolicy:refs.POLICY,referenceHash:'a'.repeat(64)};for(const v of job.variants){v.frames=Array.from({length:6},(_,i)=>({path:v.key+'_frame'+i}));v.fidelity={policy:refs.POLICY,referenceHash:job.referenceHash,assetHash:v.asset.hash,videoHash:refs.hash(Buffer.from('video'))};}return job;};
const rgb=hex=>[1,3,5].map(i=>parseInt(hex.slice(i,i+2),16));

(async()=>{
 const e=engine(),f=memory(),images={};
 // Each product has its own distinctly coloured photographs; Bunny's portrait exists only in its own image job's
 // result, and Duck and Saturn each have their own fixed-size Display proof.
 for(const [name,p] of Object.entries(P)){images[name]={};for(const [format,width,height] of [...FORMATS,...(['duck','saturn'].includes(name)?[['fixed',300,250]]:[])]){const bytes=await sharp({create:{width,height,channels:3,background:p.color}}).jpeg().toBuffer(),file='Brites_GAds_Creative/'+WS+'/'+name+'_'+format+'.jpg';f.files.set(file,bytes);images[name][format]={path:file,width,height,bytes:bytes.length,hash:e.E.creativeHash(bytes.toString('base64'))};}}
 const own=name=>new Set(Object.values(images[name]).map(a=>a.hash)),foreign=name=>new Set(Object.keys(P).filter(n=>n!==name).flatMap(n=>[...own(n)]));
 const placements=Object.keys(P).flatMap(name=>FORMATS.filter(([format])=>!(name==='bunny'&&format==='portrait')).map(([format])=>({id:name+'_'+format,groupRef:GROUP,productId:P[name].id,productIds:[P[name].id],device:'desktop',format,asset:images[name][format]})));
 const jobOf=name=>({id:'job_'+name,phase:'ready',createdAt:10,result:{copy:copyFor(name),productIds:[P[name].id],...(name==='bunny'?{assets:{portrait:images.bunny.portrait}}:{})}});
 const context={handle:'animals',name:'Animal charms',feedLabel:'US',countries:['2840'],dailyBudget:12,days:30,itemIds:Object.values(P).map(p=>p.offer),groups:[{key:'opportunity',ref:GROUP,name:'Animal charms',channel:'pmax',url:'https://britesjewelry.com/collections/animals',keywords:[],original:{},itemIds:Object.values(P).map(p=>p.offer),productIds:Object.values(P).map(p=>String(p.n))}]};
 const productDoc=(set,p)=>WSP+'/sourceSets/'+set+'/products/'+sha(p.id).slice(0,32),productRow=p=>({id:p.id,title:p.title,url:p.url,offerIds:[p.offer],itemId:p.offer,eligibleGroupRefs:[GROUP],images:[{id:'img_'+p.name}],position:p.n});
 f.docs.set(WSP,{schema:1,workspaceId:WS,sourceSetId:'s1',productsIds:Object.values(P).map(p=>p.id),context,settings:settingsOf('gecko'),messaging:{copy:copyFor('gecko'),productId:P.gecko.id,groupRef:GROUP,edited:true},job:jobOf('gecko'),productDesigns:{},placements,revision:1});
 for(const p of Object.values(P)){f.docs.set(productDoc('s1',p),productRow(p));f.docs.set(WSP+'/motionJobs/'+p.motionJob,film(p.name));for(const key of KEYS)f.files.set('Brites_GAds_Motion/'+p.motionJob+'/'+key+'.mp4',Buffer.from('film '+p.name+' '+key));}
 // Saturn also has a finished editor package (its own photographs), found only through its own scope.
 f.docs.set(WSP+'/editorAIJobs/eai_saturn',{id:'eai_saturn',phase:'ready',createdAt:20,scope:{productId:P.saturn.id,groupRef:GROUP}});
 f.docs.set(WSP+'/editorAIJobs/eai_saturn/data/result',{responsive:true,sources:[],publicationImages:FORMATS.map(([format])=>({format,asset:images.saturn[format],productIds:[P.saturn.id]}))});
 for(const name of ['duck','saturn'])f.docs.set(WSP+'/editorAIJobs/eai_'+name+'/data/ad_proofs_v1',{images:[{width:300,height:250,asset:images[name].fixed}]});
 // Mirrors googleAdsAdDesign save(): the outgoing scope is remembered in productDesigns and its job kept in history.
 function select(name,design=null){const w=f.docs.get(WSP),key=sha([w.settings.groupRef,w.settings.productId]).slice(0,32);w.productDesigns={...w.productDesigns,[key]:{settings:w.settings,messaging:w.messaging||null,jobId:w.job&&w.job.id||null}};if(w.job)f.docs.set(WSP+'/history/'+w.job.id,clone(w.job));
  const prior=w.productDesigns[sha([GROUP,P[name].id]).slice(0,32)];w.settings=prior?prior.settings:settingsOf(name);w.messaging=prior?prior.messaging:null;w.job=prior&&prior.jobId?clone(f.docs.get(WSP+'/history/'+prior.jobId)):null;
  // A first visit: the operator saves this product's messaging and its image job finishes.
  if(design){w.messaging={copy:copyFor(name),productId:P[name].id,groupRef:GROUP,edited:true};w.job=jobOf(name);}
  w.revision++;f.docs.set(WSP,w);}
 const designEngine=D.createAdDesignService({fb:()=>f,COL:{state:'Brites_GAds_State',approvals:'Brites_GAds_Approvals'},env:{},copyValid:()=>true});
 const control={enabled:true,dryRun:true,maxDailyBudgetTotal:0,budgetCurrency:'CAD'},mutations=[];
 e.bind({fb:()=>f,control:async()=>clone(control),_designEngine:()=>({editorAIStatus:async i=>[P.duck.id,P.saturn.id].includes(i.productId)?{jobId:'eai_'+(i.productId===P.duck.id?'duck':'saturn'),reviewVersion:1}:{},editorPublicationBasis:designEngine.editorPublicationBasis}),_designEngineAdapters:()=>({signAsset:async a=>'https://signed.test/'+a.path}),
  _reportContext:async()=>({budgetCurrency:'CAD',accountToday:'2026-10-02'}),merchantCenterId:async()=>555,validatePmaxAudienceResource:async()=>null,discoverPmaxAudienceResource:async()=>null,
  _saveCreativeAsset:async(ws,bytes,name,meta)=>{const p='Brites_GAds_Creative/'+ws+'/'+name+'.jpg';f.files.set(p,bytes);return {...meta,path:p,bytes:bytes.length,hash:e.E.creativeHash(bytes.toString('base64'))};},
  mutateAll:async(ops,o={})=>{mutations.push({validateOnly:!!o.validateOnly,label:o.label,ops:clone(ops)});if(!o.validateOnly)throw Error('Nothing may be published in this test.');return {};},
  merchantProducts:async()=>Object.values(P).map(p=>({itemId:p.offer})),_pmaxIsEligible:()=>true});
 // A review exactly as googleAdsAutopilot saved it before 19635c8: no sourceMode, sourceSetId or (without an editor
 // package) publicationImages, and a sourceHash of the live selection when it was sent.
 function legacyReview(name,withPackage=false){
  const w=f.docs.get(WSP),p=P[name],sourceHash=e.get('_adDesignSelectionHash')(w),copy=clone(w.messaging.copy);
  const designReview={workspaceId:WS,productId:p.id,groupRef:GROUP,productTitle:p.title,destination:p.url,formats:['landscape','portrait','square'],copy,context:clone(w.context),layoutReview:['duck','saturn'].includes(name)?{jobId:'eai_'+name,reviewVersion:1}:null,...(withPackage?{savedDesignId:null,publicationImages:FORMATS.map(([format])=>({format,asset:images[name][format],productIds:[p.id]})),artworkCopy:copy,copyEdited:false}:{})};
  const reviewHash=e.E.creativeHash({sourceHash,designReview}),id='design-review-'+reviewHash.slice(0,32);
  f.docs.set('Brites_GAds_Approvals/'+id,{type:'adDesignSubmission',summary:'Complete ad · '+p.title,status:'PENDING',vetted:false,createdAt:1,reviewHash,sourceHash,designReview,payload:{adDesign:{workspaceId:WS,productId:p.id,groupRef:GROUP},meta:{existingCampaignId:null}}});return id;}

 // 1. The reviews are sent to Approval one after another, as the operator moves Gecko → Bunny → Saturn → Duck: one
 // saved before frozen packages and one sent now for each product.
 const saved={},legacy={};
 for(const name of ['gecko','bunny','saturn','duck']){
  if(name!=='gecko')select(name,true);
  legacy[name]=legacyReview(name,name==='saturn');
  const out=await e.E.prepareAdDesignPublication({workspaceId:WS,target:'ads',formats:['square','landscape','portrait'],includeCopy:true,queueOnly:true});
  ok(out.status==='PENDING'&&/^design-review-[a-f0-9]{32}$/.test(out.approvalId)&&out.approvalId!==legacy[name],name+' is saved in Approval');saved[name]=out.approvalId;
 }
 const workspaceBefore=clone(f.docs.get(WSP)),review=id=>f.docs.get('Brites_GAds_Approvals/'+id),hashOf=id=>review(id).reviewHash,all=[...Object.values(saved),...Object.values(legacy)];
 ok(workspaceBefore.settings.productId===P.duck.id&&Object.keys(workspaceBefore.productDesigns).length===3,'Duck is selected; Gecko, Bunny and Saturn are remembered scopes');
 for(const name of Object.keys(P)){const r=review(saved[name]).designReview;
  ok(r.productId===P[name].id&&r.groupRef===GROUP&&r.sourceMode==='saved-package'&&r.sourceSetId==='s1',name+' review pins its own scope and source set');
  ok(r.publicationImages.length===3&&r.publicationImages.every(i=>own(name).has(i.asset.hash)),name+' review freezes its own three photographs');
  ok(!review(legacy[name]).designReview.sourceMode&&!review(legacy[name]).designReview.sourceSetId,name+' older review has no saved-package fields');}

 // 2. Every review shows its own images, display proofs and saved videos while Duck is selected.
 const status=id=>e.E.adDesignSubmissionStatus({id,hash:hashOf(id)});
 async function sees(id,name,label){const s=await status(id),responsive=s.images.filter(i=>i.kind==='responsive'),proofs=s.images.filter(i=>i.kind==='fixed');
  ok(s.ok&&s.sourceStale===false,label+': '+name+' preview is current');
  ok(responsive.length===3&&responsive.every(i=>own(name).has(i.hash))&&!s.images.some(i=>foreign(name).has(i.hash)),label+': '+name+' shows only its own three photographs');
  ok(proofs.length===(['duck','saturn'].includes(name)?1:0)&&proofs.every(i=>own(name).has(i.hash)),label+': '+name+' shows only its own Fixed Display proofs');
  const tile=s.styles.pmax.images.filter(i=>i.format);ok(tile.length===3&&tile.every(i=>own(name).has(i.hash)),label+': '+name+' Performance Max tile uses its own photographs');
  ok(s.videos.length===3&&s.videos.every(v=>v.url.includes(P[name].motionJob))&&s.styles.pmax.savedVideos.length===3,label+': '+name+' shows its own three saved films');
  ok(s.videoScope.productId===P[name].id&&s.videoScope.groupRef===GROUP&&!s.warnings.some(w=>/Video previews could not be loaded/.test(w)),label+': '+name+' video scope is its own');
  return s;}
 const approvalsBefore=clone(all.map(review));
 for(const name of Object.keys(P)){await sees(saved[name],name,'Duck selected');await sees(legacy[name],name,'Duck selected, older review');}
 for(const id of [saved.bunny,legacy.bunny])ok((await status(id)).images.find(i=>i.format==='portrait').hash===images.bunny.portrait.hash,'a photograph saved only in Bunny’s own image job comes from that job, never the selected product’s');
 ok(JSON.stringify(all.map(review))===JSON.stringify(approvalsBefore)&&JSON.stringify(f.docs.get(WSP))===JSON.stringify(workspaceBefore),'previews are read only');

 // 3. Publishing a non-selected product's films reads the films' own scope: Gecko's films pass the scope and
 // target checks and stop only at the Google read (a fake); a product outside the workspace is refused.
 const filmUpload=e.get('_motionPublication')(),geckoFilm=WSP+'/motionJobs/'+P.gecko.motionJob;
 f.docs.set(geckoFilm,{...publishable(film('gecko')),publication:{target:{campaignId:'555',groupRef:'customers/123/assetGroups/77'}}});
 e.bind({gaql:async()=>{throw Error('SENTINEL: the film scope checks passed');}});
 await assert.rejects(()=>filmUpload.start({workspaceId:WS,productId:P.gecko.id,groupRef:GROUP,jobId:P.gecko.motionJob,reviewHash:filmReviewHash(f.docs.get(geckoFilm))}),/SENTINEL/);checks++;
 await assert.rejects(()=>filmUpload.start({workspaceId:WS,productId:'gid://shopify/Product/999',groupRef:GROUP,jobId:P.gecko.motionJob,reviewHash:'not-reviewed'}),/another product or group/);checks++;
 f.docs.set(geckoFilm,film('gecko'));
 // The saved package of a non-selected product resolves through the review's own scope only.
 const basis=await designEngine.editorPublicationBasis({workspaceId:WS,productId:P.saturn.id,groupRef:GROUP},review(saved.saturn).designReview);
 ok(basis&&basis.publicationImages.length===3&&basis.publicationImages.every(p=>own('saturn').has(p.asset.hash)),'a review reads its own saved design package');
 await assert.rejects(()=>designEngine.editorPublicationBasis({workspaceId:WS,productId:P.saturn.id,groupRef:GROUP}),/advertised product or ad group changed/);checks++;

 // 4. Pending-review copy edits work for non-selected products and never touch the workspace. An older review is
 // converted to its own saved package as it is edited.
 const edited={...copyFor('bunny'),headlines:['Bunny Love','A Little Bunny Necklace','Gift a Bunny Pendant']};
 const updated=await e.E.updateAdDesignSubmission({id:saved.bunny,hash:hashOf(saved.bunny),copy:edited,includeVideos:true});
 ok(updated.ok&&updated.reviewHash!==approvalsBefore[1].reviewHash&&review(saved.bunny).designReview.copyEdited===true,'Bunny’s messaging is saved on its own review');
 const olderGecko=await e.E.updateAdDesignSubmission({id:legacy.gecko,hash:hashOf(legacy.gecko),copy:{...copyFor('gecko'),headlines:['Gecko Love','A Little Gecko Necklace','Gift a Gecko Pendant']},includeVideos:true}),convertedGecko=review(legacy.gecko).designReview;
 ok(olderGecko.ok&&convertedGecko.sourceMode==='saved-package'&&convertedGecko.copyEdited===true&&convertedGecko.publicationImages.every(i=>own('gecko').has(i.asset.hash)),'an older Gecko review saves its edit with its own frozen photographs');
 ok(JSON.stringify(f.docs.get(WSP))===JSON.stringify(workspaceBefore),'review edits never modify the design workspace');
 await sees(legacy.gecko,'gecko','after its edit');

 // 5. Each review prepares its own plan: its own photographs, offers and destination, never Duck's.
 const choice={styles:['pmax','responsive_display'],budgets:{pmax:10,responsive_display:5},countries:['2840'],durations:{pmax:30,responsive_display:30}};
 const prepare=(id,extra={})=>e.E.publishAdDesignSubmission({id,hash:hashOf(id),prepareOnly:true,...choice,...extra});
 async function ownPlan(id,name,label,refreshed=false){
  const out=await prepare(id),r=review(id),photos=r.pipelinePlan.payload.generatedAssets.filter(g=>!/brand logo/.test(g.asset.kind||''));
  ok(out.ok&&out.refreshed===refreshed&&r.status==='PENDING'&&r.pipelinePlan.hash===out.planHash&&r.designReview.sourceMode==='saved-package',label+': '+name+' plan is prepared');
  ok(photos.length===3&&photos.every(g=>own(name).has(g.asset.hash)),label+': '+name+' plan uploads only its own photographs');
  ok(JSON.stringify(r.pipelinePlan.payload.meta.itemIds)===JSON.stringify([P[name].offer])&&r.pipelinePlan.summary.destination===P[name].url,label+': '+name+' plan targets its own Merchant offer and product page');
  return out;}
 const plans={};
 for(const name of Object.keys(P))plans[name]=(await ownPlan(saved[name],name,'Duck selected')).planHash;
 ok(JSON.stringify(f.docs.get(WSP))===JSON.stringify(workspaceBefore),'preparing never modifies the design workspace');
 // Checked with Google (validate only): the exact bytes sent are Gecko's own photographs.
 const checked=await e.E.publishAdDesignSubmission({id:saved.gecko,hash:hashOf(saved.gecko),validateOnly:true,planHash:plans.gecko}),sent=mutations.at(-1);
 const sentPhotos=sent.ops.map(o=>o.assetOperation?.create?.imageAsset?.data).filter(Boolean).map(data=>e.E.creativeHash(data)).filter(h=>own('gecko').has(h)||foreign('gecko').has(h));
 ok(checked.ok&&sent.validateOnly&&sentPhotos.length===3&&sentPhotos.every(h=>own('gecko').has(h)),'Gecko is checked with Google using only its own photographs');

 // 6. A job in flight for Duck never blocks another product's review.
 const live=f.docs.get(WSP);live.job={...live.job,inFlight:{key:'image',requestId:'duck-request'},leaseUntil:Date.now()+600000};f.docs.set(WSP,live);
 for(const name of ['duck','gecko','bunny'])await sees(saved[name],name,'Duck in flight');
 await sees(legacy.bunny,'bunny','Duck in flight, older review');
 ok((await e.E.updateAdDesignSubmission({id:saved.gecko,hash:hashOf(saved.gecko),copy:copyFor('gecko'),includeVideos:true})).ok,'Gecko’s review saves while Duck is generating');
 ok((await prepare(saved.bunny,{budgets:{pmax:11,responsive_display:5}})).ok,'Bunny’s plan prepares while Duck is generating');
 // Approve and publish Saturn while Duck is selected and generating: every guard passes, including applyApproval's.
 e.bind({_deletedCampaignIds:async()=>{throw Error('SENTINEL: the publication guards passed');}});
 await assert.rejects(()=>e.E.publishAdDesignSubmission({id:saved.saturn,hash:hashOf(saved.saturn),planHash:plans.saturn,confirmed:true}),/SENTINEL/);
 ok(review(saved.saturn).status==='APPROVED'&&/SENTINEL/.test(review(saved.saturn).lastError),'Saturn is approved and reaches publication as its own scope');
 delete live.job.inFlight;live.job.leaseUntil=0;f.docs.set(WSP,live);

 // 7. The workspace refreshes its product sources. Without Bunny in the current source set, Bunny's older review
 // (which never recorded its source set) gives a clear message, while its saved package keeps its own source set.
 for(const p of [P.duck,P.gecko,P.saturn])f.docs.set(productDoc('s2',p),productRow(p));
 f.docs.set(WSP,{...f.docs.get(WSP),sourceSetId:'s2'});
 await assert.rejects(()=>status(legacy.bunny),/no longer part of its design workspace/);checks++;
 await sees(saved.bunny,'bunny','current sources without Bunny');
 // Bunny returns in a later refresh; every older review converts to its own saved package as its plan is prepared.
 for(const p of Object.values(P))f.docs.set(productDoc('s3',p),productRow(p));
 f.docs.set(WSP,{...f.docs.get(WSP),sourceSetId:'s3'});
 for(const name of ['duck','bunny','saturn']){await sees(legacy[name],name,'older review, current sources');await ownPlan(legacy[name],name,'older review converts',true);}
 await ownPlan(legacy.gecko,'gecko','older review converted on edit');
 for(const name of Object.keys(P)){const r=review(legacy[name]).designReview;ok(r.sourceMode==='saved-package'&&r.publicationImages.length===3&&r.publicationImages.every(i=>own(name).has(i.asset.hash))&&r.productId===P[name].id,name+' older review now freezes its own photographs');await sees(legacy[name],name,'converted');}
 // Saturn's Fixed Display artwork is made from its own reviewed proof, never Duck's.
 const fixedOut=await prepare(legacy.saturn,{styles:['pmax','responsive_display','fixed_display'],budgets:{pmax:10,responsive_display:5,fixed_display:5},durations:{pmax:30,responsive_display:30,fixed_display:30}});
 const fixed=review(legacy.saturn).pipelinePlan.payload.generatedAssets.filter(g=>g.fixed);
 const colours=await Promise.all(fixed.map(async g=>(await sharp(f.files.get(g.asset.path)).stats()).channels.slice(0,3).map(c=>c.mean)));
 ok(fixedOut.ok&&fixed.length===1&&colours.every(c=>c.every((v,i)=>Math.abs(v-rgb(P.saturn.color)[i])<12)),'Saturn’s Fixed Display artwork is its own proof');

 // 8. Gecko's design changes in the studio after submission. Its review is approved exactly as saved: it is not
 // stale and its prepared plan stays, but changed frozen media are refused.
 select('gecko');const g=f.docs.get(WSP);g.messaging={...g.messaging,copy:{...g.messaging.copy,descriptions:['Shop the gecko pendant at Brites Jewelry.','A gecko charm for every collection.']},updatedAt:2};f.docs.set(WSP,g);select('duck');
 await sees(saved.gecko,'gecko','after a studio edit');
 const first=await ownPlan(saved.gecko,'gecko','after a studio edit'),again=await prepare(saved.gecko);ok(again.cached===true&&again.planHash===first.planHash,'Gecko’s prepared plan is reused after a studio edit');
 const copyId='design-review-'+'f'.repeat(32),tampered={...clone(review(saved.gecko)),status:'APPROVED',pipelineReview:{hash:review(saved.gecko).pipelinePlan.hash}};tampered.designReview.publicationImages[0].asset.hash=images.duck.square.hash;f.docs.set('Brites_GAds_Approvals/'+copyId,tampered);
 await assert.rejects(()=>e.get('applyApproval')(copyId,clone(control)),/design changed after approval/);checks++;

 // 9. A product whose saved source no longer exists gives a clear message instead of another product's ad.
 f.docs.delete(productDoc('s1',P.bunny));
 await assert.rejects(()=>status(saved.bunny),/no longer part of its design workspace/);checks++;

 // 10. An older review edited in Approvals keeps its edit marked when conversion cannot find the artwork's own text
 // (no saved package, messaging or image job for its scope): the edited copy is never its own baseline.
 const ws=f.docs.get(WSP),geckoKey=sha([GROUP,P.gecko.id]).slice(0,32);ws.productDesigns[geckoKey]={...ws.productDesigns[geckoKey],messaging:null,jobId:null};f.docs.set(WSP,ws);
 const editedCopy={...copyFor('gecko'),headlines:['Gecko Glow','A Little Gecko Necklace','Gift a Gecko Pendant']},older={workspaceId:WS,productId:P.gecko.id,groupRef:GROUP,productTitle:P.gecko.title,destination:P.gecko.url,formats:['landscape','portrait','square'],copy:editedCopy,includeVideos:true,copyEdited:true,context:clone(ws.context),layoutReview:null};
 const olderHash=e.E.creativeHash({sourceHash:'older-selection',designReview:older}),olderId='design-review-'+olderHash.slice(0,32);
 f.docs.set('Brites_GAds_Approvals/'+olderId,{type:'adDesignSubmission',summary:'Complete ad · '+P.gecko.title,status:'PENDING',vetted:false,createdAt:1,reviewHash:olderHash,sourceHash:'older-selection',designReview:older,payload:{adDesign:{workspaceId:WS,productId:P.gecko.id,groupRef:GROUP},meta:{existingCampaignId:null}}});
 const olderPlan=await ownPlan(olderId,'gecko','older edited review converts',true);
 ok(olderPlan.ok&&review(olderId).designReview.copyEdited===true,'an older review edited in Approvals stays marked edited when its artwork text cannot be found');
 await assert.rejects(()=>prepare(olderId,{styles:['fixed_display'],budgets:{fixed_display:5},durations:{fixed_display:30}}),/edited messaging differs/);checks++;

 ok(!net.length,'no network request was made');
 console.log('PASS '+checks+' checks: each complete-ad review, frozen or saved before frozen packages, reads, edits, prepares, checks and publishes its own product scope whatever the workspace selects');
 require('./suite-guard.cjs').done();
})().catch(e=>{console.error(e);process.exit(1);});
