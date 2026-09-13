const assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
const {createPublicationService,reviewHash,safePublication,uploadUrl}=require('../../netlify/functions/googleAdsMotionPublication');
const {createVideoUpload}=require('../../netlify/functions/googleAdsVideoUpload');
const fixture=path.join(__dirname,'ad-design-workflow.cjs'),source=fs.readFileSync(fixture,'utf8').split('(async()=>{const e=await setup();')[0];
const ctx=vm.createContext({require:require('node:module').createRequire(fixture),__dirname,process,Buffer,console,Date,setTimeout,clearTimeout});vm.runInContext(source+'\nglobalThis.mem=memory;',ctx);
let checks=0;function ok(value,message){assert(value,message);checks++;}
async function setup(){
 const f=ctx.mem(),workspace=f.db.collection('Workspace').doc('design_test'),scope={workspaceId:'design_test',productId:'123',groupRef:'customers/1/assetGroups/2',jobId:'motion_'+'a'.repeat(40)};
 await workspace.set({settings:{productId:scope.productId,groupRef:scope.groupRef}});
 const job={id:scope.jobId,...scope,title:'Peach charm',destination:'https://example.test/peach',phase:'ready',quality:{pass:true,productFaithful:true},variants:['mobile_portrait','mobile_square','desktop_landscape'].map(key=>({key,format:key.split('_')[1],seconds:10,asset:{path:key,hash:key}}))},ref=workspace.collection('motionJobs').doc(job.id);await ref.set(job);
 const calls={start:0,finish:0,attach:0},D={fb:()=>f,context:async()=>({ref:workspace,w:(await workspace.get()).data()}),assertTarget:async()=>{},loadVideo:async()=>Buffer.from('video'),startUpload:async()=>({url:'https://googleads.googleapis.com/upload/'+(++calls.start)}),queryUpload:async()=>({offset:0}),finishUpload:async()=>({resourceName:'customers/1/youTubeVideoUploads/'+(++calls.finish)}),uploadState:async resourceName=>({resourceName,state:'PROCESSED',videoId:'abcdefghij'+resourceName.slice(-1)}),attach:async()=>{calls.attach++;return {accepted:true};},verify:async()=>({policyApproved:false,allVideosHaveImpressions:false})};
 return {ref,job,scope,calls,D,service:createPublicationService(D)};
}
(async()=>{
 const e=await setup();await e.service.start({...e.scope,reviewHash:reviewHash(e.job)});ok((await e.service.run(e.scope)).attached,'processed videos attached');
 await e.service.start({...e.scope,reviewHash:reviewHash(e.job)});await e.service.run(e.scope);ok(e.calls.start===3&&e.calls.finish===3&&e.calls.attach===1,'repeat approval does not duplicate uploads or attachment');
 const verified=await e.service.verify(e.scope);ok(!verified.publication.verification.policyApproved&&!verified.publication.verification.allVideosHaveImpressions,'upload acceptance never implies policy or serving');
 const safe=safePublication((await e.ref.get()).data().publication);ok(!JSON.stringify(safe).includes('sessionUrl')&&!JSON.stringify(safe).includes('reviewHash'),'private resumable sessions excluded from status');
 const q=await setup();await q.ref.update({quality:{pass:false,productFaithful:true}});await assert.rejects(()=>q.service.start({...q.scope,reviewHash:reviewHash(q.job)}),/quality/);checks++;
 await assert.rejects(()=>e.service.start({...e.scope,reviewHash:'stale'}),/Review/);checks++;
 await assert.rejects(()=>e.service.start({...e.scope,productId:'456',reviewHash:reviewHash(e.job)}),/changed/);checks++;
 const uncertain=await setup();uncertain.D.startUpload=async()=>{uncertain.calls.start++;throw Error('network outcome unknown');};await uncertain.service.start({...uncertain.scope,reviewHash:reviewHash(uncertain.job)});await uncertain.service.run(uncertain.scope);await uncertain.service.run(uncertain.scope);ok(uncertain.calls.start===1,'unknown session creation never replayed');
 const final=await setup();final.D.finishUpload=async()=>{final.calls.finish++;throw Error('lost finalize response');};await final.service.start({...final.scope,reviewHash:reviewHash(final.job)});await final.service.run(final.scope);await final.service.run(final.scope);ok(final.calls.finish===1&&final.calls.attach===0,'unknown finalize never replayed or attached');
 const pending=await setup();pending.D.uploadState=async resourceName=>({resourceName,state:'UPLOADED'});await pending.service.start({...pending.scope,reviewHash:reviewHash(pending.job)});ok((await pending.service.run(pending.scope)).processing&&pending.calls.attach===0,'YouTube processing must complete before attachment');
 const wrong=await setup();wrong.D.uploadState=async()=>({resourceName:'customers/1/youTubeVideoUploads/999',state:'PROCESSED',videoId:'abcdefghijk'});await wrong.service.start({...wrong.scope,reviewHash:reviewHash(wrong.job)});ok(!(await wrong.service.run(wrong.scope)).ok&&wrong.calls.attach===0,'wrong upload receipt rejected');
 assert.throws(()=>uploadUrl('https://attacker.test/session'));checks++;
 let sent;const upload=createVideoUpload({customerId:'1',version:'v24',headers:async()=>({Authorization:'Bearer fixture','Content-Type':'application/json'}),fetch:async(url,options)=>{sent={url,options};return {ok:true,headers:{get:name=>name==='x-goog-upload-url'?'https://googleads.googleapis.com/upload/1':null},json:async()=>({})};}});
 await upload.startUpload({bytes:123,title:'Peach charm',description:'Exact product'});const body=JSON.parse(sent.options.body);ok(body.you_tube_video_upload.video_privacy==='UNLISTED'&&!body.you_tube_video_upload.channel_id,'Google-managed upload is unlisted');ok(sent.options.redirect==='error'&&sent.options.headers['X-Goog-Upload-Header-Content-Length']==='123','bounded resumable upload metadata');
 await upload.finishUpload('https://googleads.googleapis.com/upload/1',Buffer.from('abc'),2);ok(sent.options.headers['X-Goog-Upload-Offset']==='2'&&sent.options.body.toString()==='abc','resumes from confirmed offset');
 console.log('PASS '+checks+' video upload, review scope, duplicate prevention, processing and policy checks');
})().catch(e=>{console.error(e.stack);process.exitCode=1});
