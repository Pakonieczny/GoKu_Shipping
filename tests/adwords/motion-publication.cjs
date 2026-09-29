const assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
const {createPublicationService,reviewHash,safePublication,uploadUrl}=require('../../netlify/functions/googleAdsMotionPublication');
const {createVideoUpload}=require('../../netlify/functions/googleAdsVideoUpload');
const fixture=path.join(__dirname,'ad-design-workflow.cjs'),source=fs.readFileSync(fixture,'utf8').split('(async()=>{const e=await setup();')[0].replace('update:async v=>docs.set(p,{...docs.get(p),...clone(v)})',`update:async v=>{const current=clone(docs.get(p));for(const [key,value] of Object.entries(clone(v))){const parts=key.split('.');let target=current;for(const part of parts.slice(0,-1))target=target[part]||(target[part]={});target[parts.at(-1)]=value;}docs.set(p,current);}`);
const ctx=vm.createContext({require:require('node:module').createRequire(fixture),__dirname,process,Buffer,console,Date,setTimeout,clearTimeout});vm.runInContext(source+'\nglobalThis.mem=memory;',ctx);
let checks=0;function ok(value,message){assert(value,message);checks++;}
async function setup(){
 const f=ctx.mem(),workspace=f.db.collection('Workspace').doc('design_test'),scope={workspaceId:'design_test',productId:'123',groupRef:'customers/1/assetGroups/2',jobId:'motion_'+'a'.repeat(40)};
 await workspace.set({settings:{productId:scope.productId,groupRef:scope.groupRef}});
 const job={id:scope.jobId,...scope,title:'Peach charm',destination:'https://example.test/peach',phase:'ready',quality:{pass:true,productFaithful:true,mobileReadable:true,score:98},variants:['mobile_portrait','mobile_square','desktop_landscape'].map(key=>({key,format:key.split('_')[1],seconds:10,asset:{path:key,hash:key}}))},ref=workspace.collection('motionJobs').doc(job.id);await ref.set(job);
 const calls={start:0,finish:0,attach:0},D={fb:()=>f,context:async()=>({ref:workspace,w:(await workspace.get()).data()}),assertTarget:async()=>{},loadVideo:async()=>Buffer.from('video'),startUpload:async()=>({url:'https://googleads.googleapis.com/upload/'+(++calls.start)}),queryUpload:async()=>({offset:0}),finishUpload:async()=>({resourceName:'customers/1/youTubeVideoUploads/'+(++calls.finish)}),uploadState:async resourceName=>({resourceName,state:'PROCESSED',videoId:'abcdefghij'+resourceName.slice(-1)}),attach:async()=>{calls.attach++;return {accepted:true};},verify:async()=>({policyApproved:false,allVideosHaveImpressions:false})};
 return {ref,job,scope,calls,D,service:createPublicationService(D)};
}
(async()=>{
 const engine=fs.readFileSync(path.join(__dirname,'../../netlify/functions/googleAdsAutopilot.js'),'utf8');
 const start=engine.indexOf(' const assertTarget=async(job,target=null)=>{',engine.indexOf('function _motionPublication()'));
 let liveProduct='gid://shopify/Product/123',liveStatus='PAUSED',liveOffer='shopify_us_123_456';
 const liveJob={workspaceId:'design_test',productId:liveProduct,groupRef:'customers/1/assetGroups/2',destination:'https://example.test/peach'};
 const liveCheck=vm.runInNewContext(engine.slice(start,engine.indexOf('\n const upload=',start))+';assertTarget',{CID:'1',_gaqlString:x=>JSON.stringify(x),_adDesignPublicationContext:async()=>({w:{context:{campaignId:'42'}},product:{id:liveProduct,url:liveJob.destination},group:{ref:liveJob.groupRef}}),gaql:async q=>q.includes('FROM asset_group_listing_group_filter')?[
   {assetGroupListingGroupFilter:{type:'SUBDIVISION'}},{assetGroupListingGroupFilter:{type:'UNIT_INCLUDED',caseValue:{productItemId:{value:liveOffer}}}},{assetGroupListingGroupFilter:{type:'UNIT_EXCLUDED'}}
 ]:[{campaign:{id:'42',status:liveStatus},assetGroup:{status:'ENABLED',finalUrls:[liveJob.destination]}}]});
 await liveCheck(liveJob);ok(true,'actual production guard accepts Shopify Product GID');
 liveProduct='123';await liveCheck({...liveJob,productId:'123'});ok(true,'actual production guard accepts numeric product ID');liveProduct=liveJob.productId;
 liveOffer='shopify_us_999_456';await assert.rejects(()=>liveCheck(liveJob),/filters/);checks++;liveOffer='shopify_us_123_456';
 liveStatus='ENABLED';await assert.rejects(()=>liveCheck(liveJob),/paused campaign/);checks++;liveStatus='PAUSED';
 await assert.rejects(()=>liveCheck({...liveJob,productId:'gid://shopify/ProductVariant/123'}),/product-specific/);checks++;
 await assert.rejects(()=>liveCheck({...liveJob,groupRef:'customers/9/assetGroups/2'}),/product-specific/);checks++;
 await assert.rejects(()=>liveCheck({...liveJob,destination:'https://example.test/other'}),/changed/);checks++;
 const e=await setup();await e.service.start({...e.scope,reviewHash:reviewHash(e.job)});ok((await e.service.run(e.scope)).attached,'processed videos attached');
 await e.service.start({...e.scope,reviewHash:reviewHash(e.job)});await e.service.run(e.scope);ok(e.calls.start===3&&e.calls.finish===3&&e.calls.attach===1,'repeat approval does not duplicate uploads or attachment');
 const verified=await e.service.verify(e.scope);ok(!verified.publication.verification.policyApproved&&!verified.publication.verification.allVideosHaveImpressions,'upload acceptance never implies policy or serving');
 let verifyCalls=0;e.D.verify=async()=>{verifyCalls++;return {};};await e.service.verify(e.scope);await e.service.verify(e.scope);ok(verifyCalls===0,'repeated policy checks reuse the five-minute saved result');
 const safe=safePublication((await e.ref.get()).data().publication);ok(!JSON.stringify(safe).includes('sessionUrl')&&!JSON.stringify(safe).includes('reviewHash'),'private resumable sessions excluded from status');
 for(const score of [94,96,null,101]){const low=await setup();await low.ref.update({quality:{pass:true,productFaithful:true,mobileReadable:true,score}});await assert.rejects(()=>low.service.start({...low.scope,reviewHash:reviewHash(low.job)}),/quality target/);checks++;}
 const weighted=await setup();await weighted.ref.update({quality:{rubric:'complete-ad-v2',pass:true,productFaithful:true,mobileReadable:true,score:94}});assert((await weighted.service.start({...weighted.scope,reviewHash:reviewHash(weighted.job)})).ok);checks++;
 const q=await setup();await q.ref.update({quality:{pass:false,productFaithful:true}});await assert.rejects(()=>q.service.start({...q.scope,reviewHash:reviewHash(q.job)}),/quality/);checks++;
 await assert.rejects(()=>e.service.start({...e.scope,reviewHash:'stale'}),/Review/);checks++;
 await assert.rejects(()=>e.service.start({...e.scope,productId:'456',reviewHash:reviewHash(e.job)}),/changed/);checks++;
 const uncertain=await setup();uncertain.D.startUpload=async()=>{uncertain.calls.start++;throw Error('network outcome unknown');};await uncertain.service.start({...uncertain.scope,reviewHash:reviewHash(uncertain.job)});await uncertain.service.run(uncertain.scope);await uncertain.service.run(uncertain.scope);ok(uncertain.calls.start===1,'unknown session creation never replayed');
 const final=await setup();final.D.finishUpload=async()=>{final.calls.finish++;throw Error('lost finalize response');};await final.service.start({...final.scope,reviewHash:reviewHash(final.job)});await final.service.run(final.scope);await final.service.run(final.scope);ok(final.calls.finish===1&&final.calls.attach===0,'unknown finalize never replayed or attached');
 const pending=await setup();pending.D.uploadState=async resourceName=>({resourceName,state:'UPLOADED'});await pending.service.start({...pending.scope,reviewHash:reviewHash(pending.job)});ok((await pending.service.run(pending.scope)).processing&&pending.calls.attach===0,'YouTube processing must complete before attachment');
 const wrong=await setup();wrong.D.uploadState=async()=>({resourceName:'customers/1/youTubeVideoUploads/999',state:'PROCESSED',videoId:'abcdefghijk'});await wrong.service.start({...wrong.scope,reviewHash:reviewHash(wrong.job)});ok(!(await wrong.service.run(wrong.scope)).ok&&wrong.calls.attach===0,'wrong upload receipt rejected');

 // Dry run starts no YouTube upload; the saved films are still checked.
 const dry=await setup();let dryRun=true,loads=0;dry.D.dryRun=async()=>dryRun;dry.D.loadVideo=async()=>{loads++;return Buffer.from('video');};
 await dry.service.start({...dry.scope,reviewHash:reviewHash(dry.job)});let dryOut=await dry.service.run(dry.scope),dryPub=(await dry.ref.get()).data().publication;
 ok(dryOut.dryRun&&dryOut.uploaded===false&&dry.calls.start+dry.calls.finish+dry.calls.attach===0&&loads===3&&dryPub.phase==='validated'&&dryPub.videos.every(v=>!v.sessionUrl&&!v.inFlight),'dry run starts no upload session and attaches nothing');
 ok(/No YouTube upload was started/.test(safePublication(dryPub).message),'the dry-run result says nothing was uploaded');
 dryRun=false;await dry.service.start({...dry.scope,reviewHash:reviewHash(dry.job)});ok((await dry.service.run(dry.scope)).attached&&dry.calls.start===3&&dry.calls.attach===1,'with dry run off the same approval flow uploads and attaches');
 // Nothing attached (a failed check or a rejected validate-only request): a free reset keeps the upload receipts.
 const free=await setup();let rejectFree=true;free.D.attach=async()=>{free.calls.attach++;if(rejectFree)throw Object.assign(Error('validate-only request rejected'),{nothingAttached:true});return {accepted:true};};
 await free.service.start({...free.scope,reviewHash:reviewHash(free.job)});let freeOut=await free.service.run(free.scope),freePub=(await free.ref.get()).data().publication;
 ok(!freeOut.ok&&freeOut.resettable&&freePub.phase==='blocked'&&freePub.resettable===true&&freePub.attachmentInFlight===false&&safePublication(freePub).resettable,'a rejected validation is blocked but resettable');
 rejectFree=false;await free.service.start({...free.scope,reviewHash:reviewHash(free.job)});freePub=(await free.ref.get()).data().publication;ok(freePub.phase==='queued'&&!freePub.error&&freePub.videos.every(v=>v.resourceName),'re-approval resets it and keeps the upload receipts');
 ok((await free.service.run(free.scope)).attached&&free.calls.start===3&&free.calls.finish===3&&free.calls.attach===2,'the retry attaches without uploading again');
 const unsure=await setup();unsure.D.attach=async()=>{unsure.calls.attach++;throw Error('connection reset after dispatch');};await unsure.service.start({...unsure.scope,reviewHash:reviewHash(unsure.job)});
 ok(!(await unsure.service.run(unsure.scope)).resettable&&(await unsure.ref.get()).data().publication.attachmentInFlight===true,'an attachment that may have reached Google is never resettable');
 await assert.rejects(()=>unsure.service.start({...unsure.scope,reviewHash:reviewHash(unsure.job)}),/connection reset/);checks++;
 // A worker that crashed mid-attachment left the request unresolved: a later failed check must not clear it.
 const crashed=await setup();await crashed.service.start({...crashed.scope,reviewHash:reviewHash(crashed.job)});const cp=(await crashed.ref.get()).data().publication;
 await crashed.ref.update({publication:{...cp,phase:'uploading',leaseUntil:0,attachmentInFlight:true,videos:cp.videos.map((v,i)=>({...v,resourceName:'customers/1/youTubeVideoUploads/'+(i+1),state:'PROCESSED',videoId:'abcdefghij'+(i+1)}))}});
 crashed.D.assertTarget=async()=>{throw Error('Video publication requires the exact product destination in a paused campaign.');};const crashedOut=await crashed.service.run(crashed.scope),crashedPub=(await crashed.ref.get()).data().publication;
 ok(!crashedOut.resettable&&crashedPub.phase==='blocked'&&!crashedPub.resettable&&crashedPub.attachmentInFlight===true&&crashed.calls.attach===0,'a crashed attachment stays unresolved even when a later check fails');

 const merchant=await setup();merchant.D.prepareMerchant=async()=>({reviewHash:'merchant-review',videoLinks:['https://www.youtube.com/watch?v=abcdefghij1'],identity:{offerId:'123'},before:['existing'],sourceName:'Exact product feed'});let merchantWrites=0;merchant.D.publishMerchant=async plan=>{merchantWrites++;assert.equal(JSON.stringify(plan.before),JSON.stringify(['existing']));return {status:'APPLIED'};};
 await assert.rejects(()=>merchant.service.prepareMerchant(merchant.scope),/Finish/);checks++;
 await merchant.service.start({...merchant.scope,reviewHash:reviewHash(merchant.job)});await merchant.service.run(merchant.scope);
 const prepared=await merchant.service.prepareMerchant(merchant.scope);ok(prepared.merchant.phase==='review'&&merchantWrites===0,'Merchant preparation is read-only and identifies the reviewed links');
 await assert.rejects(()=>merchant.service.publishMerchant({...merchant.scope,merchantReviewHash:'stale'}),/Review/);checks++;
 await merchant.service.publishMerchant({...merchant.scope,merchantReviewHash:'merchant-review'});await merchant.service.publishMerchant({...merchant.scope,merchantReviewHash:'merchant-review'});ok(merchantWrites===1,'accepted Merchant update is never replayed');
 await merchant.ref.update({quality:{pass:true,productFaithful:true,mobileReadable:true,score:84}});await assert.rejects(()=>merchant.service.publishMerchant({...merchant.scope,merchantReviewHash:'merchant-review'}),/quality target/);checks++;
 const nativeCopy={headlines:['Peach charm'],longHeadlines:['A little sweetness'],descriptions:['Find your Peach charm.']},nativeRows=Object.entries({headlines:'HEADLINE',longHeadlines:'LONG_HEADLINE',descriptions:'DESCRIPTION'}).flatMap(([key,fieldType])=>nativeCopy[key].map(text=>({assetGroupAsset:{fieldType},asset:{textAsset:{text}}})));
 const nativeCheck=require('../../netlify/functions/googleAdsMotionPublication').nativeCompatibility;ok(nativeCheck({plan:{nativeCopy}},nativeRows).copyMatches&&!nativeCheck({plan:{nativeCopy}},nativeRows).shopNowLinked,'matching copy cannot substitute for a native clickable action');nativeRows.push({assetGroupAsset:{fieldType:'CALL_TO_ACTION_SELECTION'},asset:{callToActionAsset:{callToAction:'SHOP_NOW'}}});ok(nativeCheck({plan:{nativeCopy}},nativeRows).shopNowLinked,'native Shop now link is required');nativeRows[0].asset.textAsset.text='Wrong message';ok(!nativeCheck({plan:{nativeCopy}},nativeRows).copyMatches,'animated publication rejects mismatched static messaging');
 const bound={...e.job,pipelineVersion:2,plan:{copy:{headline:'Peach charm'}}};ok(reviewHash(bound)!==reviewHash({...bound,plan:{copy:{headline:'Different message'}}}),'new reviews bind approved messaging');
 assert.throws(()=>uploadUrl('https://attacker.test/session'));checks++;
 let sent;const upload=createVideoUpload({customerId:'1',version:'v24',headers:async()=>({Authorization:'Bearer fixture','Content-Type':'application/json'}),fetch:async(url,options)=>{sent={url,options};return {ok:true,headers:{get:name=>name==='x-goog-upload-url'?'https://googleads.googleapis.com/upload/1':null},json:async()=>({})};}});
 await upload.startUpload({bytes:123,title:'Peach charm',description:'Exact product'});const body=JSON.parse(sent.options.body);ok(body.you_tube_video_upload.video_privacy==='UNLISTED'&&!body.you_tube_video_upload.channel_id,'Google-managed upload is unlisted');ok(sent.options.redirect==='error'&&sent.options.headers['X-Goog-Upload-Header-Content-Length']==='123','bounded resumable upload metadata');
 await upload.finishUpload('https://googleads.googleapis.com/upload/1',Buffer.from('abc'),2);ok(sent.options.headers['X-Goog-Upload-Offset']==='2'&&sent.options.body.toString()==='abc','resumes from confirmed offset');
 console.log('PASS '+checks+' video upload, review scope, duplicate prevention, processing and policy checks');
 require('./suite-guard.cjs').done();
})().catch(e=>{console.error(e.stack);process.exitCode=1});

