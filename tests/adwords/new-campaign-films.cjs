// New paused campaigns receive their product's three reviewed films. After a first ad or Campaign
// Styles publication is APPLIED, the films bind to the new asset group and the existing reviewed
// upload -> validate -> attach flow runs; dry run validates the attachment and attaches nothing.
// Offline: Firestore, Google Ads reads and writes, YouTube receipts and the worker dispatch are fakes.
const assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm'),path=require('node:path');
const dir=path.resolve(__dirname,'../../netlify/functions')+'/',file=dir+'googleAdsAutopilot.js',realRequire=require('node:module').createRequire(file),sharp=realRequire('sharp');
const {reviewHash}=realRequire('./googleAdsMotionPublication'),R=realRequire('./googleAdsCampaignStyles');
const clone=x=>x==null?x:JSON.parse(JSON.stringify(x));
let checks=0;const ok=(v,m)=>{assert(v,m);checks++;};
function engine(){const context=vm.createContext({module:{exports:{}},exports:{},require:n=>n==='node-fetch'?async()=>{throw Error('Live network forbidden');}:realRequire(n),process:{env:{GADS_CUSTOMER_ID:'123'}},console,Buffer,Date,Intl,Map,Set,URL,setTimeout,clearTimeout});vm.runInContext(fs.readFileSync(file,'utf8'),context);return {E:context.module.exports,get:n=>vm.runInContext(n,context),bind(values){context.__m=values;vm.runInContext(Object.keys(values).map(k=>k+'=__m.'+k).join('\n'),context);}};}
function memory(){const docs=new Map(),files=new Map();
 const doc=p=>({id:p.split('/').pop(),path:p,get:async()=>({exists:docs.has(p),id:p.split('/').pop(),data:()=>clone(docs.get(p))}),set:async v=>{docs.set(p,clone(v));},update:async v=>{if(!docs.has(p))throw Error('Missing document '+p);const next=clone(docs.get(p));for(const [key,value] of Object.entries(clone(v))){const parts=key.split('.');let at=next;for(const part of parts.slice(0,-1))at=at[part]||(at[part]={});at[parts.at(-1)]=value;}docs.set(p,next);},collection:n=>collection(p+'/'+n)});
 const collection=p=>({doc:n=>doc(p+'/'+n),where(){return this;},select(){return this;},get:async()=>({docs:[...docs.keys()].filter(k=>k.startsWith(p+'/')&&!k.slice(p.length+1).includes('/')).map(k=>({id:k.split('/').pop(),data:()=>clone(docs.get(k))}))})});
 return {docs,files,db:{collection,runTransaction:async fn=>fn({get:r=>r.get(),set:(r,v)=>r.set(v),update:(r,v)=>r.update(v)})},FV:{serverTimestamp:()=>Date.now()},admin:{storage:()=>({bucket:()=>({file:p=>({download:async()=>{if(!files.has(p))throw Error('Saved file missing');return [files.get(p)];},getSignedUrl:async()=>['https://storage.test/'+p]})})})}};}

const WS='ws_new',PRODUCT='11',NEW_AD='opportunity:duck',EXISTING='customers/123/assetGroups/7',DEST='https://britesjewelry.com/products/duck',JOB='motion_'+'a'.repeat(40),PUB='publish_'+'b'.repeat(32),REVIEW='design-review-'+'c'.repeat(32);
const OFFERS=['shopify_us_11_1','shopify_us_11_2'],groupOf=id=>'customers/123/assetGroups/'+id;
const copy={headlines:['Duck Necklace','A Whimsical Duck Charm','Jewelry For Duck Lovers'],longHeadlines:['Discover a whimsical duck necklace'],descriptions:['Shop the duck necklace at Brites Jewelry.','Find the duck charm necklace in our collection.']};
const films=(extra={})=>({id:JOB,workspaceId:WS,productId:PRODUCT,groupRef:NEW_AD,title:'Duck necklace',destination:DEST,phase:'ready',pipelineVersion:2,plan:{copy:{headline:'Your Little Duck'},nativeCopy:clone(copy)},quality:{pass:true,productFaithful:true,mobileReadable:true,score:98},variants:['mobile_portrait','mobile_square','desktop_landscape'].map(key=>({key,format:key.split('_')[1],seconds:10,asset:{path:'Brites_GAds_Motion/'+JOB+'/'+key+'.mp4',hash:key}})),createdAt:1000,...extra});

// campaigns: the paused campaigns Google created, each with one asset group (id) for the product.
async function setup({job=films(),groupRef=NEW_AD,campaignId=null,campaigns={555:66}}={}){
 const e=engine(),f=memory(),ws='Brites_GAds_State/adDesign/workspaces/'+WS,state={dryRun:false,mutations:[],queries:[],campaigns:{...campaigns},status:{},filters:OFFERS,dispatched:[]};
 const context={handle:'duck',feedLabel:'US',groups:[{ref:groupRef,channel:'pmax'}],itemIds:OFFERS,...(campaignId?{campaignId}:{})};
 f.docs.set(ws,{sourceSetId:'s1',settings:{productId:PRODUCT,groupRef},context,messaging:{copy}});
 f.docs.set(ws+'/sourceSets/s1/products/p11',{id:PRODUCT,title:'Duck necklace',url:DEST,offerIds:OFFERS});
 if(job)f.docs.set(ws+'/motionJobs/'+job.id,{...job,groupRef});
 const gaql=async q=>{state.queries.push(q);
  const inCampaigns=q.match(/campaign\.id IN \(([\d,]+)\)/);if(inCampaigns)return inCampaigns[1].split(',').filter(id=>state.campaigns[id]).map(id=>({campaign:{id},assetGroup:{resourceName:groupOf(state.campaigns[id]),finalUrls:[DEST]}}));
  const named=q.match(/FROM asset_group WHERE asset_group\.resource_name = '([^']+)'/);if(named){const id=Object.keys(state.campaigns).find(c=>groupOf(state.campaigns[c])===named[1]);return id?[{campaign:{id,status:state.status[id]||'PAUSED'},assetGroup:{resourceName:named[1],status:'ENABLED',finalUrls:[DEST]}}]:[];}
  if(q.includes('FROM asset_group_listing_group_filter'))return [{assetGroupListingGroupFilter:{type:'SUBDIVISION'}},...state.filters.map(value=>({assetGroupListingGroupFilter:{type:'UNIT_INCLUDED',caseValue:{productItemId:{value}}}})),{assetGroupListingGroupFilter:{type:'UNIT_EXCLUDED',caseValue:{productItemId:{}}}}];
  if(q.includes("IN ('HEADLINE'"))return [...Object.entries({headlines:'HEADLINE',longHeadlines:'LONG_HEADLINE',descriptions:'DESCRIPTION'}).flatMap(([k,fieldType])=>copy[k].map(text=>({assetGroupAsset:{fieldType},asset:{textAsset:{text}}}))),{assetGroupAsset:{fieldType:'CALL_TO_ACTION_SELECTION'},asset:{callToActionAsset:{callToAction:'SHOP_NOW'}}}];
  if(q.includes('FROM you_tube_video_upload')){const name=q.match(/'([^']+)'/)[1];return [{youTubeVideoUpload:{resourceName:name,videoId:'abcdefghij'+name.slice(-1),state:'PROCESSED'}}];}
  if(q.includes("asset_group_asset.field_type = 'YOUTUBE_VIDEO'"))return [];
  throw Error('Unexpected Google query: '+q);};
 e.bind({fb:()=>f,gaql,control:async()=>({enabled:true,dryRun:state.dryRun}),
  mutateAll:async(ops,o={})=>{const vo=o.validateOnly==null?!!(o.ctrl||{}).dryRun:o.validateOnly;state.mutations.push({vo,label:o.label,ops:clone(ops)});return vo?{}:{mutateOperationResponses:ops.map((_,i)=>({assetResult:{resourceName:'customers/123/assets/'+(900+i)}}))};},
  _adDesignSelectionHash:()=>'source',_adDesignApprovalReview:async id=>({hash:f.docs.get('Brites_GAds_Approvals/'+id)?.reviewHash,reviewed:true}),
  applyApproval:async id=>{if(state.dryRun)return {status:'VALIDATED'};await f.db.collection('Brites_GAds_Approvals').doc(id).update({status:'APPLIED',publishedCampaignIds:Object.keys(state.campaigns).concat(state.extraCampaigns||[])});return {status:'APPLIED'};},
  _verifiedCampaignAnalysisBasis:async()=>({version:2,snapshotHash:'s',snapshot:{components:{assetGroups:[]}}}),
  merchantProducts:async()=>OFFERS.map(itemId=>({itemId})),_pmaxIsEligible:()=>true});
 const scope={workspaceId:WS,productId:PRODUCT,groupRef};
 return {...e,f,state,ws,scope,job:()=>f.docs.get(ws+'/motionJobs/'+JOB),binding:()=>[...f.docs.entries()].find(([k])=>k.startsWith(ws+'/motionTargets/'))?.[1]||null,
  // A prepared first ad: the confirmation names the exact reviewed films (as prepareAdDesignPublication saves it).
  prepareFirstAd(motion){f.docs.set('Brites_GAds_Approvals/design-'+PUB,{status:'APPROVED',reviewHash:'rh'});f.docs.set(ws+'/publications/'+PUB,{id:PUB,target:'ads',status:'PENDING',reviewHash:'rh',sourceHash:'source',approvalId:'design-'+PUB,productId:PRODUCT,groupRef,selection:{formats:['square','landscape','portrait'],copy:true},...(motion?{motion}:{})});},
  publishFirstAd(){return e.E.publishAdDesignPublication({workspaceId:WS,id:PUB,hash:'rh',confirmed:true});},
  // Earlier upload receipts, so a run goes straight to YouTube processing and the attachment.
  uploaded(){const j=f.docs.get(ws+'/motionJobs/'+JOB);j.publication.videos.forEach((v,i)=>{v.resourceName='customers/123/youTubeVideoUploads/'+(i+1);v.state='UPLOADED';});f.docs.set(ws+'/motionJobs/'+JOB,j);}};
}
const motionOf=job=>({jobId:job.id,reviewHash:reviewHash(job)});

(async()=>{
 // 1. What a confirmation promises depends on the product's current films.
 let e=await setup({job:null});const plan=(job,c=copy)=>e.get('_motionFilmPlan')(e.get('_adDesignWorkspaceRef')(WS),{id:PRODUCT,url:DEST},NEW_AD,c);
 let p=await plan();ok(!p.motion&&/no animated films yet/.test(p.note),'no films: the paused campaign is flagged, never silently without films');
 for(const [job,re,label] of [[films({phase:'running',quality:null}),/still being made/,'films in progress are promised on approval'],[films({quality:{pass:true,productFaithful:true,mobileReadable:true,score:80}}),/not passed review/,'films below target are never attached'],[films({publication:{phase:'processing'}}),/own Google upload/,'films with their own upload are not moved'],[films({plan:{copy:{},nativeCopy:{...copy,headlines:['Other headline']}}}),/different messaging/,'films made for other messaging are not attached']]){
  e.f.docs.set(e.ws+'/motionJobs/'+JOB,{...job,groupRef:NEW_AD});p=await plan();ok(!p.motion&&re.test(p.note),label);}
 e.f.docs.set(e.ws+'/motionJobs/'+JOB,films());p=await plan();ok(p.motion?.jobId===JOB&&p.motion.reviewHash===reviewHash(films())&&/unlisted/.test(p.note),'ready films are named exactly in the confirmation');
 e.f.docs.set(e.ws+'/motionJobs/'+JOB+'_old',films({id:JOB+'_old',createdAt:10}));ok((await plan()).motion.jobId===JOB,'the newest reviewed films are chosen');

 // 2. Before the ad exists there is no Google group: status says so and no upload can start.
 e=await setup();let status=await e.E.adDesignMotionStatus(e.scope);
 ok(status.attachTarget===null&&/Publish this product/.test(status.attachNote),'new-ad films wait for their campaign instead of failing silently');
 await assert.rejects(()=>e.E.startAdMotionPublication({...e.scope,jobId:JOB,reviewHash:reviewHash(films())}),/Publish this product/);ok(!e.job().publication,'no upload starts without a paused campaign to attach to');

 // 3. First ad APPLIED: films bind to the new asset group and their reviewed upload is queued.
 e.prepareFirstAd(motionOf(films()));let out=await e.publishFirstAd();
 ok(out.status==='APPLIED'&&out.publishedCampaignId==='555'&&/uploading to YouTube as unlisted videos/.test(out.message),'publication reports the queued film upload');
 ok(JSON.stringify(out.motionPublication)===JSON.stringify({queued:true,workspaceId:WS,jobId:JOB,productId:PRODUCT,groupRef:NEW_AD}),'worker receives the exact film scope');
 let b=e.binding();ok(b&&b.campaignId==='555'&&b.assetGroupRef===groupOf(66)&&b.groupRef===NEW_AD&&b.source.kind==='first_ad','binding names the new paused campaign and asset group');
 ok(e.job().publication.phase==='queued'&&JSON.stringify(e.job().publication.target)===JSON.stringify({campaignId:'555',groupRef:groupOf(66)})&&e.job().publication.reviewHash===reviewHash(films()),'publication is fixed to the new group and the reviewed films');
 ok(e.state.mutations.length===0,'publishing the ad attaches nothing by itself');
 ok(e.state.queries.some(q=>q.includes("resource_name = '"+groupOf(66)+"'"))&&e.state.queries.filter(q=>q.includes('assetGroups/')).every(q=>q.includes(groupOf(66))),'the paused state, destination and product filters are checked on the new group');

 // 4. Dry run (the console switch) validates the exact attachment and attaches nothing.
 e.uploaded();e.state.dryRun=true;let run=await e.E.runAdMotionPublication({...e.scope,jobId:JOB});
 ok(run.dryRun&&run.validated&&e.job().publication.phase==='validated'&&e.job().publication.attachmentInFlight===false,'dry run ends validated, with no attachment in flight');
 ok(e.state.mutations.length===1&&e.state.mutations[0].vo===true,'dry run sends only validateOnly');
 const ops=e.state.mutations[0].ops;ok(ops.filter(o=>o.assetGroupAssetOperation).length===3&&ops.every(o=>!o.assetGroupAssetOperation||o.assetGroupAssetOperation.create.assetGroup===groupOf(66)&&o.assetGroupAssetOperation.create.fieldType==='YOUTUBE_VIDEO'),'the three films target the new asset group');
 ok(!JSON.stringify(e.state.mutations).match(/campaignOperation|campaignBudgetOperation|"status"/),'the attachment never touches campaign status or budget');
 status=await e.E.adDesignMotionStatus(e.scope);ok(status.publication.phase==='validated'&&/Dry run/.test(status.publication.message)&&status.publication.target.groupRef===groupOf(66),'status explains the dry-run result and its target');
 await e.E.runAdMotionPublication({...e.scope,jobId:JOB});ok(e.state.mutations.length===2&&e.state.mutations.every(m=>m.vo),'a repeated dry run still attaches nothing');

 // 5. Dry run off: approving again attaches the same uploads to the same paused group.
 e.state.dryRun=false;out=await e.E.startAdMotionPublication({...e.scope,jobId:JOB,reviewHash:reviewHash(films())});ok(out.queued,'re-approval queues the attachment');
 run=await e.E.runAdMotionPublication({...e.scope,jobId:JOB});const last=e.state.mutations.slice(-2);
 ok(run.attached&&e.job().publication.phase==='attached'&&last[0].vo===true&&last[1].vo===false,'validation then one real attachment');
 ok(last[1].ops.every(o=>!o.assetGroupAssetOperation||o.assetGroupAssetOperation.create.assetGroup===groupOf(66))&&e.job().publication.videos.every((v,i)=>v.resourceName==='customers/123/youTubeVideoUploads/'+(i+1)&&v.state==='PROCESSED'),'existing uploads are reused, never uploaded again');
 await e.E.runAdMotionPublication({...e.scope,jobId:JOB});ok(e.state.mutations.length===4,'an attached publication is never replayed');

 // 6. Console dry run: the campaign is only validated, so nothing binds or uploads.
 e=await setup();e.state.dryRun=true;e.prepareFirstAd(motionOf(films()));out=await e.publishFirstAd();
 ok(out.status==='VALIDATED'&&!out.motionPublication&&!e.binding()&&!e.job().publication&&!/film/.test(out.message),'validated ad leaves films untouched');

 // 7. Films not ready at publication: bound now, attachable later from Animated ads.
 e=await setup({job:films({phase:'running',quality:null})});e.prepareFirstAd(null);out=await e.publishFirstAd();
 ok(/still being made/.test(out.message)&&!out.motionPublication&&e.binding()?.assetGroupRef===groupOf(66),'unfinished films are bound and the message says what happens next');
 e.f.docs.set(e.ws+'/motionJobs/'+JOB,films());status=await e.E.adDesignMotionStatus(e.scope);
 ok(status.attachTarget?.groupRef===groupOf(66)&&status.attachTarget.campaignId==='555'&&/paused campaign 555/.test(status.attachNote),'Animated ads offers the attach step to the bound campaign');
 out=await e.E.startAdMotionPublication({...e.scope,jobId:JOB,reviewHash:reviewHash(films())});ok(out.queued&&e.job().publication.target.groupRef===groupOf(66),'the operator approval targets the new paused group');

 // 8. Films finished after the confirmation are not uploaded without their own approval.
 e=await setup();e.prepareFirstAd({jobId:JOB,reviewHash:'stale'});out=await e.publishFirstAd();
 ok(/not part of this confirmation/.test(out.message)&&!out.motionPublication&&!e.job().publication&&e.binding(),'unconfirmed films wait for approval in Animated ads');

 // 9. Safety: a campaign that is not paused, or no single product group, never receives films.
 e=await setup();e.state.status['555']='ENABLED';e.prepareFirstAd(motionOf(films()));out=await e.publishFirstAd();
 ok(out.ok&&out.status==='APPLIED'&&/not attached: Video publication requires the exact product destination in a paused campaign/.test(out.message)&&!e.job().publication,'an enabled campaign is refused and the ad result still stands');
 e=await setup({campaigns:{}});e.state.extraCampaigns=['555'];e.prepareFirstAd(motionOf(films()));out=await e.publishFirstAd();
 ok(/not linked/.test(out.message)&&!e.binding()&&!e.job().publication,'no matching asset group: nothing is bound');
 e=await setup();e.state.filters=[OFFERS[0],'shopify_us_99_1'];e.prepareFirstAd(motionOf(films()));out=await e.publishFirstAd();
 ok(/product filters/.test(out.message)&&!e.job().publication,'a group selling another product is refused');

 // 10. Campaign Styles: the plan names the films; APPLIED binds them to the new Performance Max group.
 for(const existing of [false,true]){
  e=await setup({groupRef:existing?EXISTING:NEW_AD,campaignId:existing?'42':null,campaigns:{777:88},job:films({groupRef:existing?EXISTING:NEW_AD})});e.state.extraCampaigns=['778'];
  const assets={};for(const [shape,width,height] of [['square',1200,1200],['landscape',1200,628],['portrait',960,1200]]){const bytes=await sharp({create:{width,height,channels:3,background:'#d1b284'}}).jpeg().toBuffer(),p='Brites_GAds_Creative/test/'+shape+'.jpg';assets[shape]={path:p,width,height,bytes:bytes.length,hash:e.E.creativeHash(bytes.toString('base64'))};e.f.files.set(p,bytes);}
  const w=e.f.docs.get(e.ws);w.job={result:{assets}};e.f.docs.set(e.ws,w);
  e.bind({_reportContext:async()=>({budgetCurrency:'CAD',accountToday:'2026-09-29'}),merchantCenterId:async()=>123,_saveCreativeAsset:async(ws,bytes,name,meta)=>{const p='Brites_GAds_Creative/test/'+name+'.jpg';e.f.files.set(p,bytes);return {...meta,path:p,bytes:bytes.length,hash:e.E.creativeHash(bytes.toString('base64'))};}});
  const item={sourceHash:'source',designReview:{workspaceId:WS,productId:PRODUCT,groupRef:e.scope.groupRef,copy,context:w.context}};
  const styles=await e.get('_prepareCampaignStyles')({item,context:{w,product:{id:PRODUCT,title:'Duck necklace',url:DEST,offerIds:OFFERS}},choice:R.selection(['pmax','responsive_display'],{pmax:10,responsive_display:5},['2840']),identity:'d'.repeat(64)});
  ok(JSON.stringify(styles.payload.meta.motion)===JSON.stringify(motionOf(films({groupRef:e.scope.groupRef})))&&/paused Performance Max campaign/.test(styles.summary.videoStatus)&&/Display campaigns start without them/.test(styles.summary.videoStatus),(existing?'existing':'new')+' design: the plan names the films and where they attach');
  e.f.docs.set('Brites_GAds_Approvals/'+REVIEW,{type:'adDesignSubmission',status:'PENDING',reviewHash:'rh',sourceHash:'source',designReview:item.designReview,pipelinePlan:styles,payload:styles.payload});
  out=await e.E.publishAdDesignSubmission({id:REVIEW,hash:'rh',planHash:styles.hash,confirmed:true});
  ok(out.status==='APPLIED'&&/uploading to YouTube/.test(out.message)&&out.motionPublication?.queued&&out.motionPublication.groupRef===e.scope.groupRef,(existing?'existing':'new')+' design: APPLIED Campaign Styles queue the reviewed films');
  ok(e.binding()?.campaignId==='777'&&e.binding().source.kind==='campaign_styles'&&e.job().publication.target.groupRef===groupOf(88),(existing?'existing':'new')+' design: films bind to the new Performance Max group, not the Display campaign');
 }
 status=await e.E.adDesignMotionStatus(e.scope);ok(!('attachTarget' in status)&&status.publication.target.campaignId==='777','with an upload, status shows the publication’s own fixed target');

 // 11. The worker dispatch: only a queued film upload starts the background task.
 const calls=[];const Kmod={exports:{}},K={process:{env:{URL:'https://example.invalid',EDIT_PASSCODE:'pass'}},console,Date,Set,JSON,module:Kmod,exports:Kmod.exports,require:n=>n==='node-fetch'?async(url,opts)=>{calls.push(JSON.parse(opts.body));return {ok:true,status:202};}:n==='./googleAdsAutopilot'?{control:async()=>({}),publishAdDesignPublication:async b=>b.withFilms?{ok:true,status:'APPLIED',message:'Created.',motionPublication:{queued:true,workspaceId:WS,jobId:JOB,productId:PRODUCT,groupRef:NEW_AD}}:{ok:true,status:'VALIDATED',message:'Validated.',motionPublication:null},publishAdDesignSubmission:async()=>({ok:true,status:'APPLIED',message:'Created.',motionPublication:null})}:n==='./firebaseAdmin'?(()=>{throw Error('no firebase');})():require(n)};
 vm.createContext(K);vm.runInContext(fs.readFileSync(dir+'googleAdsAutopilotKick.js','utf8'),K);
 out=await Kmod.exports.handleAction({action:'publishAdDesignPublication',withFilms:true});
 ok(calls.length===1&&calls[0].tasks[0]==='adMotionPublication'&&calls[0].jobId===JOB&&calls[0].groupRef===NEW_AD&&calls[0].token==='pass'&&out.status==='APPLIED','an APPLIED first ad dispatches its queued film upload');
 await Kmod.exports.handleAction({action:'publishAdDesignPublication'});await Kmod.exports.handleAction({action:'publishAdDesignSubmission'});ok(calls.length===1,'no queued films, no dispatch');
 // 12. Animated ads panel: where the films attach, before and after an upload exists.
 const {JSDOM}=require('jsdom');
 async function panel(status){const dom=new JSDOM('<div id="host"></div>',{runScripts:'outside-only',url:'https://example.test'}),w=dom.window;w.HTMLDialogElement.prototype.showModal=function(){this.open=true;};w.HTMLDialogElement.prototype.close=function(){this.open=false;};w.setTimeout=()=>0;w.clearTimeout=()=>{};
  w.eval(fs.readFileSync(path.join(__dirname,'../../brites-ad-motion.js'),'utf8'));const calls=[];w.BritesAdMotion.mount(w.document.getElementById('host'),{scope:{workspaceId:WS,productId:PRODUCT,groupRef:NEW_AD},request:async(a,b)=>{calls.push({a,b});return a==='adDesignMotionStatus'?status:{ok:true,queued:true};}});
  await new Promise(setImmediate);return {dom,calls,q:s=>w.document.querySelector(s)};}
 const ready={ok:true,workspaceId:WS,jobId:JOB,phase:'ready',reviewHash:'f'.repeat(64),variants:[],quality:{score:98},qualityTargetMet:true};
 let ui=await panel({...ready,attachTarget:null,attachNote:'Publish this product’s ad first. Its reviewed films attach to the new paused campaign once Google creates it.'});
 ok(ui.q('[data-publish]').hidden&&/Publish this product’s ad first/.test(ui.q('[data-publication]').textContent),'unbound new-ad films explain why no upload is offered');ui.dom.window.close();
 ui=await panel({...ready,attachTarget:{campaignId:'555',groupRef:groupOf(66)},attachNote:'These films attach to paused campaign 555, created when this ad was published.'});
 ok(!ui.q('[data-publish]').hidden&&/paused campaign 555/.test(ui.q('[data-publication]').textContent),'bound films offer the upload step');
 ui.q('[data-publish]').click();ok(/Target: paused campaign 555 · asset group 66\. The campaign stays paused\./.test(ui.q('[data-confirm]').textContent),'the approval names the exact paused campaign and group');
 await ui.q('[data-confirm] .bam-primary').onclick();ok(ui.calls.some(c=>c.a==='startAdMotionPublication'&&c.b.jobId===JOB&&c.b.reviewHash==='f'.repeat(64)),'approving starts the reviewed upload');ui.dom.window.close();
 ui=await panel({...ready,publication:{phase:'validated',message:'Dry run: Google validated attaching these videos. Nothing was attached. Turn off dry run, then approve the upload again to attach them.',target:{campaignId:'555',groupRef:groupOf(66)},videos:[]}});
 ok(/Dry run/.test(ui.q('[data-publication]').textContent)&&/Target: paused campaign 555/.test(ui.q('[data-publication]').textContent)&&!ui.q('[data-publish]').hidden,'a dry-run result explains itself and can be approved again');ui.dom.window.close();
 console.log('PASS '+checks+' new-campaign film binding, dry-run attachment, target safety and dispatch checks');
})().catch(e=>{console.error(e.stack);process.exitCode=1;});
