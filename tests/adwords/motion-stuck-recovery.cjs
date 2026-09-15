// Reproduce the exact stuck job from production and prove it recovers.
const assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm'),sharp=require('sharp');
const {createMotionService}=require('../../netlify/functions/googleAdsAdMotion');
const fixture=path.join(__dirname,'ad-design-workflow.cjs'),source=fs.readFileSync(fixture,'utf8').split('(async()=>{const e=await setup();')[0];
const ctx=vm.createContext({require:require('node:module').createRequire(fixture),__dirname,process,Buffer,console,Date,setTimeout,clearTimeout});
vm.runInContext(source+'\nglobalThis.mem=memory;',ctx);
(async()=>{
 const f=ctx.mem(),ref=f.db.collection('Workspace').doc('design_test'),scope={productId:'p',groupRef:'g'},id='eai_'+'a'.repeat(40);
 const jpeg=await sharp({create:{width:100,height:100,channels:3,background:'#b69b74'}}).jpeg().toBuffer();
 await ref.set({settings:scope,editorAI:{id},context:{groups:[{ref:'g'}]}});
 const e=ref.collection('editorAIJobs').doc(id);await e.set({scope,phase:'ready',createdAt:1});
 await e.collection('data').doc('request').set({sources:[{asset:{path:'photo'}}]});
 await e.collection('data').doc('result').set({responsive:{plan:{copy:{headline:'For your favorite gator fan',shortHeadline:'Crocodile Charm',description:'Gift-ready packaging',cta:'Shop now'},nativeCopy:{headlines:['Crocodile Charm','For your favorite gator fan']},style:{background:'#fff7ee',ink:'#30291f',accent:'#a67c35',headlineFont:'Georgia'},layouts:[]}},sources:[{asset:{path:'photo'},width:100,height:100}]});
 const calls={create:0,download:0,review:0,render:0};
 const blobs=new Map();
 const D={fb:()=>f,context:async()=>({ref,w:(await ref.get()).data(),products:[{id:'p',title:'Crocodile Charm Pendant',url:'https://example.test/p'}]}),
  loadAsset:async()=>jpeg,sampleMotionFrames:async()=>[],
  planMotion:async()=>{throw Error('MUST NOT make a new planning request');},
  videoRequest:async(r,m)=>{if(m==='POST'){calls.create++;throw Error('MUST NOT generate a new film');}return {id:'v1_x',status:'completed'};},
  videoContent:async()=>{calls.download++;return Buffer.from('mp4');},
  saveVideo:async(i,b,k,info)=>{blobs.set(k,b);return {path:k,hash:k,...info};},
  loadVideo:async a=>blobs.get(a.path)||Buffer.from('mp4'),signVideo:async a=>'https://x/'+a.path,
  renderVariants:async(b,orientation,plan)=>{calls.render++;
   // Exercise the real caption engine so a stuck square composition would still throw here.
   const policy=require('../../brites-ad-format-policy'),{captionLayers}=require('../../netlify/functions/googleAdsAdMotion');
   const out=[];
   for(const format of policy.video.formats){
    const master=format.key==='portrait'?'portrait':format.key==='landscape'?'landscape':(plan.squareMaster||'landscape');
    if(master!==orientation)continue;
    const layers=await captionLayers({...plan,sourceOrientation:orientation},format);
    out.push({key:(format.key==='landscape'?'desktop':'mobile')+'_'+format.key,device:format.key==='landscape'?'desktop':'mobile',format:format.key,width:format.width,height:format.height,seconds:10,bytes:b,frames:[jpeg,jpeg,jpeg],composition:{mode:layers.mode}});
   }
   return out;},
  reviewImages:async()=>{calls.review++;return {productFaithful:true,mobileReadable:true,pass:true,score:94,scores:{messaging:95,layout:92,relevance:95,visualAppeal:93,productRecognition:100},categoryReviews:{},issues:[]};}};
 const service=createMotionService(D);
 const start=await service.start({workspaceId:'design_test',...scope});
 // Recreate the exact stored production state: both films paid for and saved,
 // framing measured, square master never chosen, stopped by the old rule.
 const jobRef=ref.collection('motionJobs').doc(start.jobId);
 await jobRef.update({phase:'needs_attention',compositionBlocked:true,inFlight:null,leaseUntil:0,owner:null,quality:null,squareMaster:null,
  creativeDirection:Object.fromEntries(['rationale','setting','props','lighting','opening','middle','ending','portrait','landscape','identity','limitations'].map(k=>[k,'saved '+k])),
  composition:{portrait:{x:.05,y:.05,w:.9,h:.9,note:'large subject'},landscape:{x:.02,y:.05,w:.95,h:.9,note:'large subject'}},
  masters:{portrait:{id:'v1_p',status:'completed',progress:100,size:'720x1280',asset:{path:'m_portrait',hash:'m_portrait'}},landscape:{id:'v1_l',status:'completed',progress:100,size:'1280x720',asset:{path:'m_landscape',hash:'m_landscape'}}},
  error:'Square film has insufficient clear space in either saved master for large messaging and the complete jewelry. Re-run with more clear space beside the product.',
  progress:{pct:65,label:'Animation interrupted · saved work retained'}});
 const stuck=await service.status({workspaceId:'design_test',...scope});
 assert.equal(stuck.autoResume,true,'the stuck job must advertise automatic resume');
 const resumed=await service.start({workspaceId:'design_test',...scope,resumeJobId:stuck.jobId});
 assert.equal(resumed.queued,true,'resume must queue the saved job');
 let r;for(let i=0;i<4;i++){r=await service.run({workspaceId:'design_test',jobId:stuck.jobId});if(!r.continue)break;}
 assert(r.ok,'run must finish: '+r.error);
 const done=await service.status({workspaceId:'design_test',...scope});
 assert((await jobRef.get()).data().squareMaster,'a square master is chosen instead of stopping');
 assert(done.compositionNotes.length,'each fallback is reported to the operator');
 assert.equal(done.phase,'ready');assert.equal(done.variants.length,3);
 assert.equal(calls.create,0,'no new film may be generated');
 assert.equal(calls.review,1,'the recovered set is reviewed once');
 console.log('PASS the exact stuck production job recovers to three reviewed films with no new video charge');
})().catch(e=>{console.error(e.stack||e);process.exit(1)});
