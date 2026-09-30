const assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm'),sharp=require('sharp');
const {requestBody,outputVideo,createGeminiVideo,MODEL}=require('../../netlify/functions/googleAdsGeminiVideo');
const {createMotionService,renderVariants}=require('../../netlify/functions/googleAdsAdMotion');
const fixture=path.join(__dirname,'ad-design-workflow.cjs'),source=fs.readFileSync(fixture,'utf8').split('(async()=>{const e=await setup();')[0];
const ctx=vm.createContext({require:require('node:module').createRequire(fixture),__dirname,process,Buffer,console,Date,setTimeout,clearTimeout});vm.runInContext(source+'\nglobalThis.mem=memory;',ctx);
let n=0;function ok(v,m){assert(v,m);n++;}
async function setup(){const f=ctx.mem(),ref=f.db.collection('Workspace').doc('design_test'),scope={productId:'p',groupRef:'g'},id='eai_'+'a'.repeat(40),jpeg=await sharp({create:{width:100,height:100,channels:3,background:'#b69b74'}}).jpeg().toBuffer();
 await ref.set({settings:scope,editorAI:{id},context:{groups:[{ref:'g'}]}});const e=ref.collection('editorAIJobs').doc(id);await e.set({scope,phase:'ready',createdAt:1});await e.collection('data').doc('request').set({sources:[{asset:{path:'photo'}}]});await e.collection('data').doc('result').set({responsive:{plan:{nativeCopy:{headlines:['Pendant','A personal touch']},layouts:[]}},sources:[{asset:{path:'photo'},width:100,height:100}]});
 const direction=Object.fromEntries(['rationale','setting','props','lighting','opening','middle','ending','portrait','landscape','identity','limitations'].map(k=>[k,'Verified '+k]));
 const blobs=new Map(),calls={create:0,download:0,review:0,render:0,aspects:[],bodies:[],renders:[]},D={fb:()=>f,context:async()=>({ref,w:(await ref.get()).data(),products:[{id:'p',title:'Pendant',url:'https://example.test/p'}]}),loadAsset:async()=>jpeg,sampleMotionFrames:async()=>[],planMotion:async request=>{if(request.text.format.name==='brites_motion_treatment'){const brief=request.input[0].content;
   ok(brief.includes('Choose a fresh product-specific scene')&&brief.includes('never overpower it')&&brief.includes('exact colour and finish of the catalog reference'),'motion planning keeps the charm dominant and its metal faithful');
   ok(/ONE CAMERA IDEA, NAMED IN camera/.test(brief)&&/rack focus/.test(brief)&&/Never a plain push-in, a zoom out or a highlight pass/.test(brief),'the treatment must choose a distinct cinematic approach per piece');
   ok((brief.match(/^RULE \d+ - /gm)||[]).length===8&&/These film rules override any conflicting guidance above/.test(brief),'the treatment brief reads as numbered rules that override earlier guidance');
   ok(/It never orbits, spins, rotates or turns the jewelry/.test(brief)&&!/gentle orbit/.test(brief)&&!/crane-like rise/.test(brief),'no planned camera move may require inventing an unseen part of the piece');
   ok(/RULE 1 - THE PIECE IS NEVER ALTERED\. This outranks every other rule/.test(brief)&&/No re-modelling in three dimensions/.test(brief)&&/no added bail, stone or chain/.test(brief),'the treatment is told the piece may never be altered');
   ok(/148 pixels wide/.test(brief)&&/with messaging beside it about 40 pixels clear/.test(brief),'the treatment reserves the smaller wordmark with messaging beside it');
   ok(/RULE 2 - THE LOOK IS BRIGHT AND CHEERFUL/.test(brief)&&/Never dark, dim, moody, overcast or gloomy/.test(brief)&&/Subordinate does not mean washed out/.test(brief),'the treatment must plan a bright, cheerful, vibrant scene');
   ok(/Decide per piece/.test(brief)&&/Most films carry no model at all/.test(brief),'a worn moment is case specific, never the house style');
   ok(request.text.format.schema.required.includes('camera'),'the saved treatment records its camera approach');}return {output_text:JSON.stringify(request.text.format.name==='video_product_bounds'?{portrait:{bounds:[.35,.48,.3,.24],complete:true,confidence:.99,note:'fixture'},landscape:{bounds:[.6,.25,.25,.5],complete:true,confidence:.99,note:'fixture'},square:{bounds:[.45,.3,.28,.55],complete:true,confidence:.99,note:'fixture'}}:direction)};},videoRequest:async(route,method,body)=>{if(method==='POST'){calls.create++;calls.aspects.push(body.response_format.aspect_ratio);calls.bodies.push(body);const prompt=body.input.map(v=>v.text||'').join(' ');
   ok(/warm yellow gold stays warm, rich, luminous gold/.test(prompt)&&/Never silver, white, grey, green-tinted, chalky or desaturated/.test(prompt),'video generation is told to hold the exact gold of the reference');
   ok(/RULE 1 - NEVER ALTER THE JEWELRY\. This outranks every other line here/.test(prompt),'not altering the piece is stated first as the overriding rule');
   ok(/No re-modelling in three dimensions\. No rotating, turning, tilting or flipping\. No edge, side or back the reference does not show/.test(prompt),'re-modelling and rotating the piece are forbidden outright');
   ok(/No thickened, bevelled, rounded, smoothed or re-cut edges\. No redrawn engraving\. No added loop, bail, stone or chain/.test(prompt),'edges, engraving and added parts are protected');
   ok(/^RULE \d+ - /m.test(prompt)&&(prompt.match(/^RULE \d+ - /gm)||[]).length===5,'the film prompt reads as short numbered rules rather than one dense block');
   ok(/hold the piece still and move the scene instead/.test(prompt),'a camera idea never justifies inventing part of the piece');
   ok(/RULE 2 - BRIGHT AND CHEERFUL/.test(prompt)&&/Never dark, dim, moody, overcast, gloomy or heavy with shadow/.test(prompt),'the film is required to be bright and cheerful, never gloomy');
   ok(/Never grey, muddy, hazy, foggy, misty, bloomed or milky/.test(prompt)&&/Never quiet it by draining its colour or going dark/.test(prompt),'the scene stays subordinate through craft, not by washing it out or darkening it');
   ok(/Perform the camera approach named in the treatment, not a generic push-in or zoom/.test(prompt)&&/never orbit, spin, rotate or turn the jewelry itself/.test(prompt),'the camera moves through the scene and never around the piece');
   ok(!/Coordinate every shot with these actual overlay colours/.test(prompt),'the brand palette is never handed over as colour codes');
   // The film model draws any noun it is shown, prohibition or not, so the prompt
   // must not name overlays or lettering even to forbid them.
   ok(!/\b(?:text|lettering|letters?|words?|caption|captions|subtitles?|headlines?|messaging|message|typography|typeface|fonts?|glyphs?|logos?|wordmark|watermark|signage|labels?|tags?|overlay|print(?:ed|ing)?|writing|written|slogan|tagline)\b/i.test(prompt),'the film prompt never names lettering or overlays, in any form');
   ok(/EVERY SURFACE IS PLAIN AND UNMARKED/.test(prompt)&&/blank and unmarked/.test(prompt),'the film is told positively that every surface is bare');ok(body.model===MODEL&&body.background&&body.response_format.aspect_ratio,'exact Gemini interaction request');return {id:'v1_'+calls.create,status:'completed',steps:[{type:'model_output',content:[{type:'video',uri:'https://storage.googleapis.com/video.mp4'}]}]};}throw Error('Unexpected polling of completed result');},videoContent:async()=>{calls.download++;return Buffer.from('mp4')},saveVideo:async(id,b,key,info)=>{blobs.set(key,b);return {path:key,hash:key,...info}},loadVideo:async a=>blobs.get(a.path),signVideo:async a=>'https://example.test/'+a.path,renderVariants:async(b,orientation,plan)=>{calls.render++;calls.renders.push({orientation,plan});return ['mobile','desktop'].flatMap(device=>['portrait','square','landscape'].filter(format=>(!plan?.pipelineVersion||device===(format==='landscape'?'desktop':'mobile'))).filter(format=>(plan?.pipelineVersion>=3?format:format==='square'?(plan?.squareMaster||(device==='mobile'?'portrait':'landscape')):format)===orientation).map(format=>({key:device+'_'+format,device,format,width:720,height:720,seconds:10,bytes:b,frames:[jpeg,jpeg,jpeg]})))},reviewImages:async()=>{calls.review++;return {productFaithful:true,mobileReadable:true,pass:true,score:98,issues:[]}}};return {f,ref,D,calls,scope,id,service:createMotionService(D)};
}
(async()=>{
 const body=requestBody(Buffer.from('image'),'Exact jewelry, subtle camera movement','portrait');ok(body.model==='gemini-omni-1.1-flash'&&body.response_format.aspect_ratio==='9:16','requested model and ratio');ok(!('seconds'in body)&&!('input_reference'in body),'no OpenAI video parameters sent to Gemini');ok(outputVideo({steps:[{type:'user_input',content:[{type:'video',data:'wrong'}]},{type:'model_output',content:[{type:'video',data:'right'}]}]}).data==='right','only generated video returned');
 let sent=0;const provider=createGeminiVideo({apiKey:'fixture',fetch:async(url,opts)=>{sent++;ok(opts.headers['x-goog-api-key']==='fixture'&&!url.includes('fixture'),'key kept in header');return {ok:true,json:async()=>({id:'v1_1'}),headers:{get:()=>null}}}});await provider.request('interactions','POST',body);await assert.rejects(()=>provider.content({uri:'https://attacker.test/video'}),/unsupported/);ok(sent===1,'no credentials or fetch to untrusted download host');
 const e=await setup(),start=await e.service.start({workspaceId:'design_test',...e.scope});const result=await e.service.run({workspaceId:'design_test',jobId:start.jobId});ok(result.ok,'complete motion pipeline '+result.error);const status=await e.service.status({workspaceId:'design_test',...e.scope});ok(status.phase==='ready'&&status.variants.length===3,'three previews saved');ok(e.calls.create===3&&e.calls.render===3&&e.calls.review===1,'three purpose-built masters, one per ratio');await e.service.run({workspaceId:'design_test',jobId:start.jobId});ok(e.calls.create===3,'completed job never charges twice');
  {
   // Version 3: portrait, square and landscape each have their own film request.
   const job=(await e.ref.collection('motionJobs').doc(start.jobId).get()).data();
   ok(job.pipelineVersion===3&&job.renderVersion===11&&require('../../netlify/functions/googleAdsAdMotion').PIPELINE===3,'new jobs are pipeline version 3, render version 11');
   ok(e.calls.aspects.join()==='9:16,16:9,16:9','the provider is asked for 9:16, then 16:9 twice (Gemini has no 1:1)');
   ok(Object.keys(job.masters).join()==='portrait,square,landscape'&&job.masters.square.size==='1280x720'&&job.masters.portrait.size==='720x1280'&&job.masters.landscape.size==='1280x720','each master keeps its own size; the square master is a full 1280x720 film');
   ok(new Set(Object.values(job.masters).map(m=>m.id)).size===3&&Object.values(job.masters).every(m=>m.estimatedUsd>1&&m.estimatedUsd<1.1),'three separate provider jobs, each receipted at one master of cost');
   ok(job.variants.map(v=>v.key+':'+v.master).join()==='mobile_portrait:portrait,mobile_square:square,desktop_landscape:landscape','each format is cut from the master made for it');
   ok(!job.squareMaster,'no square master is borrowed from another film');
   ok(Math.abs(job.estimatedUsd-3*1.0136)<.02,'estimated cost covers three masters plus reviews ('+job.estimatedUsd+')');
   ok(e.calls.renders.map(r=>r.orientation).join()==='portrait,square,landscape'&&e.calls.renders.every(r=>r.plan.pipelineVersion===3&&r.plan.renderVersion===11),'each master is composed once with the version 3 plan');
   ok(e.calls.renders.every(r=>r.plan.composition&&r.plan.composition.w>0),'each master carries its own measured bounds');
   const sq=e.calls.renders.find(r=>r.orientation==='square').plan.composition;ok(sq.x>=.21875&&sq.x+sq.w<=.78125,'the square master is measured over the full frame and its jewelry lies in the central square');
   ok(/9:16/.test(e.calls.bodies[0].input[1].text)&&/16:9 frame\. Only its middle/.test(e.calls.bodies[1].input[1].text)&&/Fill the 16:9 frame\. Keep the whole piece toward the right/.test(e.calls.bodies[2].input[1].text),'each film request states its own framing');
   ok(job.creativeDirection.square&&/central square/.test(job.creativeDirection.square),'a saved treatment without a square direction derives one');
   ok(/Verified portrait/.test(e.calls.bodies[0].input[1].text)&&/central square/.test(e.calls.bodies[1].input[1].text)&&!/Verified landscape/.test(e.calls.bodies[1].input[1].text)&&/Verified landscape/.test(e.calls.bodies[2].input[1].text),'each film gets its own staging direction');
   const st=await e.service.status({workspaceId:'design_test',...e.scope});
   ok(st.expectedExports===3&&st.masterProgress.map(m=>m.format).join()==='portrait,square,landscape'&&st.masterProgress.every(m=>m.status==='completed'&&m.progress===100)&&!st.canRecompose,'status reports all three masters');
  }
  {
   // A treatment that names a square staging uses it for the square film only.
   const s3=await setup(),base=s3.D.planMotion;s3.D.planMotion=async r=>{if(r.text.format.name==='brites_motion_treatment'){ok(r.text.format.schema.required.includes('square')&&r.text.format.schema.properties.square,'a version 3 treatment must direct the square film');ok(/Square: the piece large and centred/.test(r.input[0].content)&&/Each of the three sizes is filmed on its own/.test(r.input[0].content)&&/portrait, square and landscape fields/.test(r.input[0].content),'the treatment brief asks for a square direction');const out=JSON.parse((await base(r)).output_text);out.square='Verified square staging';return {output_text:JSON.stringify(out)};}return base(r);};
   const j3=await s3.service.start({workspaceId:'design_test',...s3.scope});ok((await s3.service.run({workspaceId:'design_test',jobId:j3.jobId})).ok,'pipeline runs with a square direction');
   const text=b=>b.input[1].text;ok(/Verified square staging/.test(text(s3.calls.bodies[1]))&&!/Verified square staging/.test(text(s3.calls.bodies[0]))&&!/Verified square staging/.test(text(s3.calls.bodies[2])),'the square direction reaches only the square film');
  }
  {
   // Resume reuses every completed master and buys only the missing ones.
   const r3=await setup(),rs=await r3.service.start({workspaceId:'design_test',...r3.scope}),rr=r3.ref.collection('motionJobs').doc(rs.jobId);
   await rr.update({masters:{portrait:{id:'v1_saved_portrait',status:'completed',progress:100,output:{uri:'https://storage.googleapis.com/saved.mp4'},size:'720x1280',requestId:'saved',estimatedUsd:1.0136}}});
   ok((await r3.service.run({workspaceId:'design_test',jobId:rs.jobId})).ok,'resumed job completes');
   ok(r3.calls.create===2&&r3.calls.aspects.join()==='16:9,16:9','a saved portrait master is reused: only the square and landscape films are bought');
   const rj=(await rr.get()).data();ok(rj.masters.portrait.id==='v1_saved_portrait'&&rj.variants.length===3,'the saved master is untouched and still feeds its format');
   await r3.service.run({workspaceId:'design_test',jobId:rs.jobId});ok(r3.calls.create===2,'completed resume never charges again');
   const c3=await setup(),cs=await c3.service.start({workspaceId:'design_test',...c3.scope}),cr=c3.ref.collection('motionJobs').doc(cs.jobId),done={status:'completed',progress:100,output:{uri:'https://storage.googleapis.com/saved.mp4'},requestId:'saved',estimatedUsd:1.0136};
   await cr.update({masters:{portrait:{...done,id:'v1_p',size:'720x1280'},square:{...done,id:'v1_s',size:'1280x720'},landscape:{...done,id:'v1_l',size:'1280x720'}}});
   ok((await c3.service.run({workspaceId:'design_test',jobId:cs.jobId})).ok&&c3.calls.create===0,'three saved masters need no provider request at all');
   // A master that is still generating is polled, never requested again.
   const p3=await setup(),ps=await p3.service.start({workspaceId:'design_test',...p3.scope}),pr=p3.ref.collection('motionJobs').doc(ps.jobId);let polls=[];
   await pr.update({masters:{portrait:{...done,id:'v1_p',size:'720x1280'},square:{id:'v1_pending',status:'in_progress',progress:0,size:'1280x720',requestId:'saved',estimatedUsd:1.0136}}});
   const inner=p3.D.videoRequest;p3.D.videoRequest=async(route,method,body)=>{if(method!=='POST'&&route==='interactions/v1_pending'){polls.push(route);return {id:'v1_pending',status:'completed',steps:[{type:'model_output',content:[{type:'video',uri:'https://storage.googleapis.com/pending.mp4'}]}]};}return inner(route,method,body);};
   ok((await p3.service.run({workspaceId:'design_test',jobId:ps.jobId})).ok&&p3.calls.create===1&&p3.calls.aspects.join()==='16:9'&&polls.length===1,'the pending square master is polled and only the missing landscape master is requested');
  }
  {
   // An unsaved provider outcome for any master stays protected.
   const q3=await setup(),qs=await q3.service.start({workspaceId:'design_test',...q3.scope}),qr=q3.ref.collection('motionJobs').doc(qs.jobId);
   await qr.update({masters:{portrait:{id:'v1_p',status:'completed',progress:100,output:{uri:'https://storage.googleapis.com/saved.mp4'},size:'720x1280'}},creativeDirection:Object.fromEntries(['rationale','setting','props','lighting','opening','middle','ending','portrait','square','landscape','identity','limitations'].map(k=>[k,'saved '+k])),inFlight:{orientation:'square',requestId:'unknown-outcome',at:1}});
   const out=await q3.service.run({workspaceId:'design_test',jobId:qs.jobId});ok(out.ok===false&&/no confirmed provider ID/.test(out.error)&&q3.calls.create===0,'an unconfirmed square request is never repeated');
  }
  {
   // Pipeline version 2 jobs stay exactly as they were saved: two masters, a chosen square master, the same maths.
   const o=await setup(),os=await o.service.start({workspaceId:'design_test',...o.scope}),or=o.ref.collection('motionJobs').doc(os.jobId);
   await or.update({pipelineVersion:2,renderVersion:10,estimatedUsd:2*10*5792*17.5/1e6});
   const before=await o.service.status({workspaceId:'design_test',...o.scope});ok(before.expectedExports===3&&before.pipelineVersion===2,'a version 2 job is unchanged before it runs');
   ok((await o.service.run({workspaceId:'design_test',jobId:os.jobId})).ok,'a version 2 job still runs');
   const oj=(await or.get()).data(),ost=await o.service.status({workspaceId:'design_test',...o.scope});
   ok(o.calls.create===2&&o.calls.render===2&&o.calls.aspects.join()==='9:16,16:9'&&o.calls.review===1,'a version 2 job buys exactly two masters (9:16 and 16:9)');
   ok(Object.keys(oj.masters).join()==='portrait,landscape'&&['portrait','landscape'].includes(oj.squareMaster)&&oj.variants.length===3&&ost.masterProgress.length===2&&ost.expectedExports===3,'a version 2 job keeps two masters and a borrowed square master');
   ok(oj.variants.filter(v=>v.master===oj.squareMaster).length===2&&!oj.variants.some(v=>v.master==='square'),'a version 2 square is still cropped from its portrait or landscape master');
   ok(o.calls.renders.every(r=>r.plan.pipelineVersion===2&&r.plan.renderVersion===10)&&!oj.creativeDirection.square,'a version 2 job is composed with its own versions and gets no square direction');
   ok(!/central square|Only its middle/.test(JSON.stringify(o.calls.bodies.map(b=>b.input[1].text))),'no square staging reaches a version 2 film');
   ok(Math.abs(oj.estimatedUsd-2*1.0136)<.02,'a version 2 job estimates two masters of cost ('+oj.estimatedUsd+')');
   await o.service.run({workspaceId:'design_test',jobId:os.jobId});ok(o.calls.create===2,'a completed version 2 job never charges twice');
   // The same design opened again finds its saved version 2 film instead of buying three more masters.
   const legacyDesign=await setup(),legacyId='motion_'+require('crypto').createHash('sha256').update(legacyDesign.id+':v2').digest('hex').slice(0,40);
   await legacyDesign.ref.collection('motionJobs').doc(legacyId).set({id:legacyId,pipelineVersion:2,renderVersion:10,productId:'p',groupRef:'g',workspaceId:'design_test',phase:'ready',createdAt:1});
   const reopened=await legacyDesign.service.start({workspaceId:'design_test',...legacyDesign.scope});ok(reopened.jobId===legacyId&&(await legacyDesign.ref.collection('motionJobs').get()).docs.length===1&&legacyDesign.calls.create===0,'a saved version 2 film is returned, never duplicated');
  }
  {
   // Old treatments stay valid; a version 3 treatment may carry a square direction.
   const motion=require('../../netlify/functions/googleAdsAdMotion'),keys=['rationale','setting','props','lighting','opening','middle','ending','portrait','landscape','identity','limitations'],old=Object.fromEntries(keys.map(k=>[k,'saved '+k]));
   ok(!('square' in motion.validateDirection(old))&&motion.validateDirection({...old,square:' Square staging '}).square==='Square staging','a saved treatment without a square direction stays valid');
   ok(!('square' in motion.resolveDirection(old,{pipelineVersion:2,plan:{}}).direction)&&/central square/.test(motion.resolveDirection(old,{pipelineVersion:3,plan:{}}).direction.square)&&motion.resolveDirection({...old,square:'Mine'},{pipelineVersion:3,plan:{}}).direction.square==='Mine','only version 3 derives or keeps a square direction');
   ok(/central square/.test(motion.resolveDirection({},{pipelineVersion:3,plan:{}}).direction.square)&&motion.resolveDirection({},{pipelineVersion:3,plan:{}}).note,'an unusable treatment still yields a square direction for version 3');
   ok(!motion.motionRequest({pipelineVersion:2}).text.format.schema.required.includes('square')&&motion.motionRequest({pipelineVersion:3}).text.format.schema.required.includes('square')&&!/Square:/.test(motion.motionRequest({pipelineVersion:2}).input[0].content),'only the version 3 treatment schema and brief carry the square staging');
   const prompt=motion.motionPrompt({title:'Pendant',plan:{},creativeDirection:Object.fromEntries(keys.map(k=>[k,'clean '+k]))},'square','');
   ok(/Fill the 16:9 frame\. Only its middle/.test(prompt)&&/completely inside that middle area/.test(prompt)&&/central square/.test(prompt)&&(prompt.match(/^RULE \d+ - /gm)||[]).length===5,'the square film is told to keep the piece inside the middle of a 16:9 frame');
   ok(!/\b(?:text|lettering|letters?|words?|caption|captions|subtitles?|headlines?|messaging|message|typography|typeface|fonts?|glyphs?|logos?|wordmark|watermark|signage|labels?|tags?|overlay|print(?:ed|ing)?|writing|written|slogan|tagline)\b/i.test(prompt)&&!/\d+\s?(?:px|pixels)/.test(prompt),'the square film prompt never names lettering, overlays or pixel sizes');
   const video=require('../../netlify/functions/googleAdsGeminiVideo'),ratio=o=>video.requestBody(Buffer.from('i'),'p',o).response_format.aspect_ratio;
   ok(ratio('square')==='16:9'&&ratio('portrait')==='9:16'&&ratio('landscape')==='16:9','the square film is requested at 16:9');
   assert.throws(()=>requestBody(Buffer.from('i'),'p','wide'),/supported video orientation/);n++;
  }
 const truncated=await setup(),ts=await truncated.service.start({workspaceId:'design_test',...truncated.scope}),tr=truncated.ref.collection('motionJobs').doc(ts.jobId);
 await tr.update({phase:'failed',inFlight:{key:'direction',requestId:'old'},leaseUntil:0});
 await tr.collection('receipts').doc('direction').set({response:{status:'incomplete',incomplete_details:{reason:'max_output_tokens'}},at:1});
 await truncated.service.start({workspaceId:'design_test',...truncated.scope,resumeJobId:ts.jobId});
 ok(!(await tr.collection('receipts').doc('direction').get()).exists,'explicit resume removes only the truncated active receipt');
 ok((await tr.collection('receipts').get()).docs.length===1,'truncated paid receipt is archived');
 ok(truncated.calls.create===0,'resume itself does not charge');
 ok((await tr.get()).data().inFlight===null,'new text step can run without reusing incomplete response');
 for(const key of ['quality','layout']){
  const x=await setup(),a=await x.service.start({workspaceId:'design_test',...x.scope}),r=x.ref.collection('motionJobs').doc(a.jobId);
  await r.update({phase:'needs_attention',inFlight:{key,requestId:'saved'},compositionBlocked:key==='layout',leaseUntil:0});
  await r.collection('receipts').doc(key).set({response:{status:'incomplete',incomplete_details:{reason:'max_output_tokens'}}});
  ok((await x.service.status({workspaceId:'design_test',...x.scope})).canResume,'confirmed truncated '+key+' offers resume');
  await x.service.start({workspaceId:'design_test',...x.scope,resumeJobId:a.jobId});
  ok(!(await r.collection('receipts').doc(key).get()).exists,'truncated '+key+' receipt is archived instead of reused');
 }
 const discardCase=await setup(),ds=await discardCase.service.start({workspaceId:'design_test',...discardCase.scope}),dr=discardCase.ref.collection('motionJobs').doc(ds.jobId);
 await dr.update({phase:'needs_attention',inFlight:{requestId:'unknown-paid-request'},masters:{portrait:{id:'saved-video'}}});
 const discardInput={workspaceId:'design_test',...discardCase.scope,discardJobId:ds.jobId,confirmDiscard:true};
 await assert.rejects(()=>discardCase.service.start({...discardInput,confirmDiscard:false}),/Confirm/);
 await assert.rejects(()=>discardCase.service.start({...discardInput,productId:'other'}),/another product/);
 await discardCase.service.start(discardInput);await discardCase.service.start(discardInput);
 const discarded=(await dr.get()).data();ok(discarded.resetAt&&discarded.phase==='cancelled'&&discarded.masters.portrait.id==='saved-video'&&discarded.inFlight.requestId==='unknown-paid-request','discard releases attempt while preserving paid evidence');
 const fresh=await discardCase.service.start({workspaceId:'design_test',...discardCase.scope});
 ok(fresh.jobId!==ds.jobId&&(await discardCase.service.start({workspaceId:'design_test',...discardCase.scope})).jobId===fresh.jobId,'fresh job after discard is distinct and idempotent');
 ok(discardCase.calls.create===0,'discard and new job reservation never call the video provider');
 const pinned=await setup(),authority=await sharp({create:{width:100,height:100,channels:3,background:'#ff0000'}}).jpeg().toBuffer(),requestRef=pinned.ref.collection('editorAIJobs').doc(pinned.id).collection('data').doc('request');
 await requestRef.set({sources:[{asset:{path:'earlier-generated-scene'}}],identitySources:[{asset:{path:'verified-catalog-photo'}}]});const oldLoad=pinned.D.loadAsset;pinned.D.loadAsset=async asset=>asset.path==='verified-catalog-photo'?authority:oldLoad(asset);
 const pinnedRequest=pinned.D.videoRequest;pinned.D.videoRequest=async(route,method,body)=>{if(method==='POST'){const {data}=await sharp(Buffer.from(body.input[0].data,'base64')).raw().toBuffer({resolveWithObject:true});ok(data[0]>240&&data[1]<20,'video generation uses original catalog identity pixels');}return pinnedRequest(route,method,body);};
 pinned.D.reviewImages=async source=>{ok(source.equals(authority),'animation review uses pinned catalog pixels instead of earlier generated artwork');return {productFaithful:true,mobileReadable:true,pass:true,score:98,issues:[]};};
 const pinnedStart=await pinned.service.start({workspaceId:'design_test',...pinned.scope});ok((await pinned.service.run({workspaceId:'design_test',jobId:pinnedStart.jobId})).ok,'catalog identity survives the complete animation pipeline');ok((await requestRef.get()).data().sources[0].asset.path==='earlier-generated-scene','original editor context remains unchanged');
 const low=await setup();low.D.reviewImages=async()=>({pass:true,productFaithful:true,mobileReadable:true,score:96,issues:['Mobile framing needs refinement']});const lowStart=await low.service.start({workspaceId:'design_test',...low.scope});const lowRun=await low.service.run({workspaceId:'design_test',jobId:lowStart.jobId});ok(lowRun.ok,'subtarget films remain available to preview');const lowStatus=await low.service.status({workspaceId:'design_test',...low.scope});ok(lowStatus.variants.length===3&&!lowStatus.qualityTargetMet&&lowStatus.canRepair,'subtarget films remain saved with an honest target status');
 const u=await setup();u.D.videoRequest=async()=>{u.calls.create++;throw Error('unknown network outcome')};const us=await u.service.start({workspaceId:'design_test',...u.scope});await u.service.run({workspaceId:'design_test',jobId:us.jobId});await u.service.run({workspaceId:'design_test',jobId:us.jobId});ok(u.calls.create===1,'unknown paid request never repeated');
 const nav=await setup(),ns=await nav.service.start({workspaceId:'design_test',...nav.scope});await nav.ref.update({settings:{productId:'other',groupRef:'other'}});ok((await nav.service.run({workspaceId:'design_test',jobId:ns.jobId})).ok,'navigation does not stop original animation');await assert.rejects(()=>nav.service.status({workspaceId:'design_test',productId:'other',groupRef:'other',jobId:ns.jobId}),/another product/);n++;
 const rejected=await setup();rejected.D.reviewImages=async()=>({productFaithful:true,pass:false,issues:['Product obscured in mobile crop']});const rejectedStart=await rejected.service.start({workspaceId:'design_test',...rejected.scope});const rejectedRun=await rejected.service.run({workspaceId:'design_test',jobId:rejectedStart.jobId});ok(rejectedRun.ok&&(await rejected.service.status({workspaceId:'design_test',...rejected.scope})).phase==='ready','quality findings do not block preview access');
 const failed=await rejected.service.status({workspaceId:'design_test',...rejected.scope}),parentBefore=JSON.stringify((await rejected.ref.collection('motionJobs').doc(failed.jobId).get()).data());
 ok(failed.canRepair&&failed.repairReviewHash,'failed review exposes a bound repair choice');await assert.rejects(()=>rejected.service.start({workspaceId:'design_test',...rejected.scope,repairOf:failed.jobId,repairReviewHash:'stale'}),/review changed/);n++;
 const repairInput={workspaceId:'design_test',...rejected.scope,repairOf:failed.jobId,repairReviewHash:failed.repairReviewHash},repair=await rejected.service.start(repairInput),repeated=await rejected.service.start(repairInput);ok(repair.jobId===repeated.jobId&&rejected.calls.create===3,'repeated repair approval creates one durable job without charging');
 ok(JSON.stringify((await rejected.ref.collection('motionJobs').doc(failed.jobId).get()).data())===parentBefore,'repair preserves original clips, review and paid history');
 rejected.D.reviewImages=async()=>({productFaithful:true,mobileReadable:true,pass:true,score:98,issues:[]});ok((await rejected.service.run({workspaceId:'design_test',jobId:repair.jobId})).ok,'repair can pass a fresh quality review');ok(rejected.calls.create===6,'one repair generates exactly three additional masters');await rejected.service.run({workspaceId:'design_test',jobId:repair.jobId});ok(rejected.calls.create===6,'completed repair never charges twice');
 const repairStatus=await rejected.service.status({workspaceId:'design_test',...rejected.scope,jobId:repair.jobId});ok(repairStatus.repairOf===failed.jobId&&!repairStatus.canRepair&&repairStatus.variants.length===3,'repaired job stays linked to original and cannot trigger unbounded retries');
 const repairedRef=rejected.ref.collection('motionJobs').doc(repair.jobId);await repairedRef.update({phase:'needs_attention',quality:{pass:false,issues:['Still failed']}});const second=await rejected.service.status({workspaceId:'design_test',...rejected.scope,jobId:repair.jobId});await assert.rejects(()=>rejected.service.start({...repairInput,repairOf:repair.jobId,repairReviewHash:second.repairReviewHash}),/bounded repair/);n++;
 ok(second.canPhotoMotion,'failed generated repair offers photograph motion');
 const photoInput={workspaceId:'design_test',...rejected.scope,photoMotionOf:repair.jobId,repairReviewHash:second.repairReviewHash};
 await assert.rejects(()=>rejected.service.start({...photoInput,repairReviewHash:'stale'}),/review changed/);n++;
 await assert.rejects(()=>rejected.service.start({...photoInput,productId:'other'}),/another product/);n++;
 const beforePhoto=JSON.stringify((await repairedRef.get()).data()),photoStart=await rejected.service.start(photoInput);
 ok((await rejected.service.start(photoInput)).jobId===photoStart.jobId,'photograph approval is idempotent');
 ok((await rejected.service.start({workspaceId:'design_test',...rejected.scope,resumeJobId:photoStart.jobId})).jobId===photoStart.jobId,'resume selects the exact photograph job instead of the original paid generation');
 await assert.rejects(()=>rejected.service.start({workspaceId:'design_test',...rejected.scope,resumeJobId:repair.jobId}),/reviewed correction/);n++;
 rejected.D.reviewImages=async()=>{rejected.calls.review++;return {productFaithful:true,mobileReadable:true,pass:true,score:98,issues:[],estimatedUsd:.18};};
 ok((await rejected.service.run({workspaceId:'design_test',jobId:photoStart.jobId})).ok,'photograph pipeline renders and reviews saved stills');
 const photoStatus=await rejected.service.status({workspaceId:'design_test',...rejected.scope,jobId:photoStart.jobId});
 ok(photoStatus.variants.length===3&&photoStatus.motionMode==='photograph'&&photoStatus.estimatedUsd===.18,'three photograph variants report only the actual review estimate');
 ok(rejected.calls.create===6&&rejected.calls.download===6,'photograph motion makes no video generation or provider download requests');
 ok(!photoStatus.canRepair&&!photoStatus.canPhotoMotion,'completed photograph does not offer another paid repair');
 await rejected.service.run({workspaceId:'design_test',jobId:photoStart.jobId});
 ok(rejected.calls.create===6&&JSON.stringify((await repairedRef.get()).data())===beforePhoto,'repeat execution preserves previous videos and costs');
 const photoRef=rejected.ref.collection('motionJobs').doc(photoStart.jobId);await photoRef.update({phase:'needs_attention',quality:{pass:false,productFaithful:true,score:72,issues:['Too much empty space']}});
 const widePhoto=await rejected.service.status({workspaceId:'design_test',...rejected.scope,jobId:photoStart.jobId});ok(widePhoto.canPhotoMotion&&!widePhoto.canRepair,'failed photograph framing offers one crop correction without generative repair');
 const closeInput={workspaceId:'design_test',...rejected.scope,photoMotionOf:photoStart.jobId,repairReviewHash:widePhoto.repairReviewHash},closeStart=await rejected.service.start(closeInput);
 ok((await rejected.service.start(closeInput)).jobId===closeStart.jobId,'closer framing is idempotent');
 ok((await rejected.service.run({workspaceId:'design_test',jobId:closeStart.jobId})).ok&&rejected.calls.create===6,'closer framing does not call the video provider');
 const closeRef=rejected.ref.collection('motionJobs').doc(closeStart.jobId);await closeRef.update({phase:'needs_attention',quality:{pass:false,score:80,issues:['Still needs art direction']}});
 const closeStatus=await rejected.service.status({workspaceId:'design_test',...rejected.scope,jobId:closeStart.jobId});ok(closeStatus.motionMode==='photograph-close'&&!closeStatus.canPhotoMotion&&!closeStatus.canRepair,'failed closer crops cannot launch an unbounded retry chain');
 await assert.rejects(()=>rejected.service.start({...closeInput,photoMotionOf:closeStart.jobId,repairReviewHash:closeStatus.repairReviewHash}),/completed animation review/);n++;
 const current=await setup(),prior=await setup();await current.ref.update({context:{campaignId:'42',groups:[{ref:'g'}]}});await prior.ref.update({context:{campaignId:'42',groups:[{ref:'g'}]}});
 const oldStart=await prior.service.start({workspaceId:'design_test',...prior.scope});await prior.service.run({workspaceId:'design_test',jobId:oldStart.jobId});
 current.D.relatedContexts=async()=>[{ref:prior.ref,w:(await prior.ref.get()).data()}];
 const recovered=await current.service.status({workspaceId:'new_version',...current.scope});
 ok(recovered.jobId===oldStart.jobId&&recovered.workspaceId==='design_test'&&recovered.fromEarlierVersion&&recovered.variants.length===3,'new ad version reads paid films from their original owning workspace');
 ok(current.calls.create===0&&current.calls.review===0,'recovering a previous-version animation makes no paid requests');
 const priorDoc=(await prior.ref.get()).data();
 for(const patch of [{context:{campaignId:'other'}},{settings:{productId:'other',groupRef:'g'}},{settings:{productId:'p',groupRef:'other'}},{archivedAt:1}]){
  await prior.ref.set({...priorDoc,...patch});ok((await current.service.status({workspaceId:'new_version',...current.scope})).phase==='idle','other campaign, product, group or archived workspace cannot supply a film');
 }
 await prior.ref.set(priorDoc);ok((await current.service.status({workspaceId:'new_version',...current.scope,jobId:oldStart.jobId})).phase==='idle','explicit missing job IDs are not silently redirected to another workspace');
 const {JSDOM}=require('jsdom'),dom=new JSDOM('<div id="motion"></div>',{runScripts:'outside-only',url:'https://example.test'}),win=dom.window,requests=[];
 win.HTMLDialogElement.prototype.showModal=function(){this.open=true;};win.HTMLDialogElement.prototype.close=function(){this.open=false;};win.setTimeout=()=>0;win.eval(fs.readFileSync(path.join(__dirname,'../../brites-ad-motion.js'),'utf8'));
 win.BritesAdMotion.mount(win.document.getElementById('motion'),{scope:{workspaceId:'new_version',...current.scope},request:async(action,payload)=>{requests.push({action,payload});return {...recovered,canRepair:true,quality:{pass:false,score:77,issues:['Preserve the exact leaf']},repairReviewHash:'review'};}});
 await new Promise(resolve=>setImmediate(resolve));
 ok(win.document.querySelector('video').getAttribute('crossorigin')==='anonymous','saved animation player requests CORS before loading the video');
 ok(win.document.querySelector('[data-detail]').textContent.includes('earlier ad version'),'animation provenance is visible to the operator');
 ok(!win.document.querySelector('button[data-device]')&&!win.document.querySelector('[data-photo-motion]'),'redundant device and photograph controls removed');ok(win.document.querySelector('[data-generate]').textContent==='Re-run animation'&&!win.document.querySelector('[data-generate]').disabled,'prominent rerun remains available after review');win.document.querySelector('[data-generate]').click();ok(!win.document.querySelector('dialog'),'rerun confirms in place without stacking a pop-up');await [...win.document.querySelectorAll('[data-confirm] button')].find(b=>b.textContent==='Re-run animation').onclick();
 ok(requests.find(r=>r.action==='startAdDesignMotion').payload.workspaceId==='design_test','repair addresses the original saved job rather than the new version');
 ok(requests.filter(r=>r.action==='adDesignMotionStatus').every(r=>r.payload.workspaceId==='new_version'),'status remains bound to the currently selected product workspace');
 const preview=await win.BritesAdMotion.openAllSizes({scope:current.scope,request:async()=>recovered,status:recovered});ok(preview.querySelectorAll('video').length===3,'all-size preview includes precisely the three relevant ratios');preview.remove();
 ok(win.document.querySelector('progress').hidden,'completed review hides the active progress bar');
 const parentRef=e.ref.collection('motionJobs').doc(start.jobId),parentData=(await parentRef.get()).data(),twoMasters={portrait:parentData.masters.portrait,landscape:parentData.masters.landscape};await parentRef.update({pipelineVersion:2,renderVersion:2,masters:twoMasters,composition:{portrait:parentData.composition.portrait,landscape:parentData.composition.landscape},squareMaster:'portrait',variants:parentData.variants.map(v=>v.master==='square'?{...v,master:'portrait'}:v)});const old=await e.service.status({workspaceId:'design_test',...e.scope});ok(old.canRecompose,'legacy captions can be updated from saved masters');const before=JSON.stringify((await parentRef.get()).data());
 const input={workspaceId:'design_test',...e.scope,recomposeOf:start.jobId,repairReviewHash:old.repairReviewHash};const composed=await e.service.start(input);await e.service.run({workspaceId:'design_test',jobId:composed.jobId});ok(e.calls.create===3&&e.calls.download===3,'caption repair never regenerates or redownloads masters');ok(JSON.stringify((await parentRef.get()).data())===before,'caption repair preserves the original job');ok((await e.service.start(input)).jobId===composed.jobId,'caption retry is idempotent');
 const composedStatus=await e.service.status({workspaceId:'design_test',...e.scope,jobId:composed.jobId});ok(!composedStatus.canRecompose,'current captions do not offer redundant correction');
 const rerunInput={workspaceId:'design_test',...e.scope,rerunOf:composed.jobId,repairReviewHash:composedStatus.repairReviewHash};const rerun=await e.service.start(rerunInput);ok((await e.service.start(rerunInput)).jobId===rerun.jobId,'explicit rerun is idempotent per saved generation');await e.service.run({workspaceId:'design_test',jobId:rerun.jobId});ok(e.calls.create===6,'explicit rerun of an older job creates three new masters');const rerunJob=(await e.ref.collection('motionJobs').doc(rerun.jobId).get()).data();ok(rerunJob.pipelineVersion===3&&rerunJob.renderVersion===11&&Object.keys(rerunJob.masters).join()==='portrait,square,landscape','a rerun of a two-master job makes the new three-master film');
 const unclear=await setup(),unclearPlan=unclear.D.planMotion;unclear.D.planMotion=async r=>r.text.format.name==='video_product_bounds'?{output_text:JSON.stringify({portrait:{bounds:[.3,.3,.2,.2],complete:false,confidence:.6,note:'occluded'}})}:unclearPlan(r);
 const unclearJob=await unclear.service.start({workspaceId:'design_test',...unclear.scope});await unclear.service.run({workspaceId:'design_test',jobId:unclearJob.jobId});const unclearState=await unclear.service.status({workspaceId:'design_test',...unclear.scope});ok(unclearState.phase==='ready'&&unclearState.variants.length===3&&unclearState.compositionNotes.length>=1&&unclear.calls.review===1,'unsure framing never stops the film: the directed region protects the jewelry, the note reaches the review');
 ok((await unclear.service.start({workspaceId:'design_test',...unclear.scope,rerunOf:unclearJob.jobId,repairReviewHash:unclearState.repairReviewHash})).queued,'explicit rerun remains available after an assumed framing');
 // Targeted fixes from the review: only the named film, captions or message change.
 const fx=await setup(),fxPlan=fx.D.planMotion;let copyFixes=0;fx.D.planMotion=async r=>r.text?.format?.name==='brites_motion_copy_fix'?(copyFixes++,{output_text:JSON.stringify({headline:'For the foodie who has everything',shortHeadline:'Pendant',description:'Gift-ready',cta:'Shop now',rationale:'specific hook'})}):fxPlan(r);
 fx.D.reviewImages=async(source,files,brief)=>{fx.calls.review++;fx.lastBrief=brief;return {pass:false,productFaithful:true,mobileReadable:true,score:85,scores:{messaging:90,layout:85,relevance:100,visualAppeal:90,productRecognition:100},categoryReviews:{messaging:{summary:'',deductions:[{points:10,reason:'Generic hook',evidence:'mobile_portrait at 0.3s',correction:'Rewrite the opening line as a specific gift hook',kind:'required',formats:['mobile_portrait']}]},layout:{summary:'',deductions:[{points:15,reason:'Caption too small',evidence:'mobile_square at 3.5s',correction:'Enlarge the caption',kind:'required',formats:['mobile_square']}]},visualAppeal:{summary:'',deductions:[{points:10,reason:'Flat lighting',evidence:'desktop_landscape frames',correction:'Add a travelling reflection across the metal',kind:'optional',formats:['desktop_landscape']}]},relevance:{summary:'',deductions:[]},productRecognition:{summary:'',deductions:[]}},issues:[]};};
 const fxStart=await fx.service.start({workspaceId:'design_test',...fx.scope});await fx.service.run({workspaceId:'design_test',jobId:fxStart.jobId});const fxState=await fx.service.status({workspaceId:'design_test',...fx.scope});
 ok(fxState.phase==='ready'&&fxState.canFix&&fxState.fixOptions.length===3,'every reviewed deduction offers one targeted fix');
 const byKind=Object.fromEntries(fxState.fixOptions.map(o=>[o.kind,o]));ok(byKind.copy?.category==='messaging'&&byKind.caption?.formats.join()==='mobile_square'&&byKind.master?.orientation==='landscape'&&byKind.master.estimatedUsd>1&&byKind.master.estimatedUsd<1.1,'copy, caption and single-film fixes are classified with honest scope and cost');
 await assert.rejects(()=>fx.service.start({workspaceId:'design_test',...fx.scope,fixOf:fxStart.jobId,repairReviewHash:'stale',fix:{category:'layout',index:0}}),/review changed/);n++;
 const captionFix={workspaceId:'design_test',...fx.scope,fixOf:fxStart.jobId,repairReviewHash:fxState.repairReviewHash,fix:{category:'layout',index:0}},cf=await fx.service.start(captionFix);
 ok(cf.queued&&(await fx.service.start(captionFix)).jobId===cf.jobId&&cf.jobId!==fxStart.jobId,'caption fix is a distinct, idempotent job');
 const beforeCaption={...fx.calls};ok((await fx.service.run({workspaceId:'design_test',jobId:cf.jobId})).ok,'caption fix completes');
 const cfState=await fx.service.status({workspaceId:'design_test',...fx.scope,jobId:cf.jobId});
 ok(cfState.phase==='ready'&&cfState.fixOf===fxStart.jobId&&cfState.variants.length===3&&fx.calls.create===beforeCaption.create&&fx.calls.download===beforeCaption.download&&fx.calls.review===beforeCaption.review+1,'caption fix re-composes without any new film or download and re-reviews the set');
 ok(cfState.variants.filter(v=>v.key!=='mobile_square').every(v=>fxState.variants.some(o=>o.key===v.key&&o.asset.hash===v.asset.hash)),'unaffected films keep their exact saved bytes');
 ok(fx.lastBrief.targetedFix?.formats.join()==='mobile_square'&&fx.lastBrief.fixNote,'the review is told which format was corrected');
 ok(cfState.estimatedUsd===0&&(await fx.ref.collection('motionJobs').doc(cf.jobId).get()).data().captionHints.square.preferBand,'caption fix costs nothing and records its hint');
 const masterFix={workspaceId:'design_test',...fx.scope,fixOf:fxStart.jobId,repairReviewHash:fxState.repairReviewHash,fix:{category:'visualAppeal',index:0}},mf=await fx.service.start(masterFix),beforeMaster={...fx.calls};
 ok((await fx.service.run({workspaceId:'design_test',jobId:mf.jobId})).ok,'master fix completes');const mfState=await fx.service.status({workspaceId:'design_test',...fx.scope,jobId:mf.jobId});
 ok(fx.calls.create===beforeMaster.create+1&&mfState.variants.length===3&&mfState.variants.find(v=>v.key==='mobile_portrait').asset.hash===fxState.variants.find(v=>v.key==='mobile_portrait').asset.hash,'master fix regenerates exactly one film and keeps the portrait film');
 ok(Math.abs(mfState.estimatedUsd-1.015)<.01,'master fix reports one film of video cost');
 const copyFix={workspaceId:'design_test',...fx.scope,fixOf:fxStart.jobId,repairReviewHash:fxState.repairReviewHash,fix:{category:'messaging',index:0}},cpf=await fx.service.start(copyFix),beforeCopy={...fx.calls};
 ok((await fx.service.run({workspaceId:'design_test',jobId:cpf.jobId})).ok,'copy fix completes');const cpfState=await fx.service.status({workspaceId:'design_test',...fx.scope,jobId:cpf.jobId}),cpfJob=(await fx.ref.collection('motionJobs').doc(cpf.jobId).get()).data();
 ok(copyFixes===1&&fx.calls.create===beforeCopy.create&&cpfState.variants.length===3&&cpfJob.plan.copy.headline==='For the foodie who has everything'&&cpfJob.copyFixed,'copy fix revises the message once and re-composes every format without new video');
 await fx.service.run({workspaceId:'design_test',jobId:cpf.jobId});ok(copyFixes===1&&fx.calls.create===beforeCopy.create,'completed fixes never charge twice');
 ok((await fx.service.status({workspaceId:'design_test',...fx.scope})).jobId===cpf.jobId,'the latest fix becomes the current animation');
 // Nothing the video model could render as text is ever put in front of it.
 {
  const motion=require('../../netlify/functions/googleAdsAdMotion');
  const keys=['rationale','setting','props','lighting','opening','middle','ending','portrait','landscape','identity','limitations'];
  const poisoned=Object.fromEntries(keys.map(k=>[k,'clean '+k]));
  poisoned.setting='Riverbank graded #58695F with Open Sans titles reading "Crocodile Charm Pendant" at 44px and 148 pixels of margin';
  poisoned.camera='Rack focus onto the piece';
  const job={title:'Crocodile Charm Pendant',plan:{style:{background:'#eef1ec',ink:'#252729',accent:'#a67c35',headlineFont:'Georgia',bodyFont:'Open Sans'},copy:{headline:'Crocodile Charm Pendant',cta:'Shop now'},nativeCopy:{headlines:['Gift-ready packaging included.']}},creativeDirection:poisoned};
  const prompt=motion.motionPrompt(job,'portrait','');
  ok(!/#[0-9a-fA-F]{3,8}/.test(prompt),'no colour code reaches the video model, even via the treatment');
  ok(!/Georgia|Open Sans/.test(prompt),'no font name reaches the video model, even via the treatment');
  ok(!/Gift-ready packaging/.test(prompt),'no saved ad copy reaches the video model, even via the treatment');
  ok(!/Crocodile|Pendant|Brites/i.test(prompt),'the product name and brand never reach the video model, which renders names it is given');
  ok(/blank and unmarked/.test(prompt)&&/No panels, cards, plaques, frames, tiles, badges, icons, buttons or rectangles/.test(prompt),'the scene is defined positively as bare, and the shapes an overlay would take are named as scenery');
  ok(/The scenery reaches all four edges/.test(prompt)&&/No dark strip along any edge, no vignette/.test(prompt),'the footage may never frame itself with bars or a border');
  ok(!/\d+\s?(?:px|pixels)/.test(prompt),'no pixel specification reaches the video model, even via the treatment');
  // Naming the thing you do not want is how it gets drawn, so the prompt must
  // not contain the vocabulary at all, not even inside a prohibition.
  ok(!/\b(?:text|lettering|letters?|words?|caption|captions|subtitles?|headlines?|messaging|message|typography|typeface|fonts?|glyphs?|logos?|wordmark|watermark|signage|labels?|tags?|overlay|print(?:ed|ing)?|writing|written|slogan|tagline)\b/i.test(prompt),'the film prompt never names lettering or overlays, in any form');
  ok(motion.scrubText('Leave the top third clear for the headline overlay, water rippling below.')==='water rippling below.','a director who describes the overlay has that clause removed before the film model sees it');
  ok(motion.scrubText('A sunlit desk with an open book beside the piece, warm linen underneath.')==='warm linen underneath.','a prop that carries writing in real life is removed before the film model sees it');
  ok(/flat stamped sheet/.test(prompt)&&/thin drawn line, never a visible band, wall or rim of metal/.test(prompt)&&/lit and shaded side, it is too thick/.test(prompt)&&/engraving is cut into the metal/.test(prompt),'the film judges thinness by a single visible criterion and keeps engraving cut into the metal');
  ok(/45-55% of frame height/.test(prompt)||/55-65% of frame width/.test(prompt),'the film is told how large the piece should sit in frame, so it is never shot distant');
  ok(/RULE 3 - ONE CONTINUOUS TAKE/.test(prompt)&&/No cuts, no jump cuts, no dissolves/.test(prompt),'the film must be one unbroken take');
  ok(/It is not the first frame/.test(prompt)&&/identical from the first frame to the last/.test(prompt),'the reference photo is never the opening frame and the piece is whole throughout');
  ok(motion.scrubDirection({title:'Crocodile Charm Pendant',plan:{},creativeDirection:Object.fromEntries(keys.map(k=>[k,'The Crocodile Charm Pendant by Brites on a stone']))},'portrait').setting==='The piece by the brand on a stone','the product name is swapped for a neutral word rather than left as a renderable token');
  const payload=JSON.stringify(motion.motionRequest(job).input[1].content);
  ok(!/#eef1ec|#252729|Georgia|Open Sans/.test(payload),'the treatment is not shown the palette, fonts or layout spec either');
 }

 // A film re-reads the operator's framing at generation time, so a crop applied
 // after the static ad was designed reaches the video without re-buying scenes.
 const refresh=await setup(),editorRequest=refresh.ref.collection('editorAIJobs').doc(refresh.id).collection('data').doc('request');
 await editorRequest.set({sources:[{asset:{path:'photo'}}],identitySources:[{id:'pinned',asset:{path:'uncropped-original'},width:1024,height:1024}]});
 let identityCalls=0;
 refresh.D.identityFor=async(workspaceId,jobId)=>{identityCalls++;assert.equal(jobId,refresh.id);return [{id:'pinned',asset:{path:'operator-crop'},width:600,height:500,framedFrom:'pinned',frame:{x:200,y:300,width:600,height:500}}];};
 const savedRequest=JSON.stringify((await editorRequest.get()).data());
 const freshFilm=await refresh.service.start({workspaceId:'design_test',...refresh.scope});
 ok((await refresh.ref.collection('motionJobs').doc(freshFilm.jobId).get()).data().originalSources[0].asset.path==='operator-crop'&&identityCalls===1,'a new film references the operator crop instead of the reference pinned earlier');
 ok(JSON.stringify((await editorRequest.get()).data())===savedRequest,'re-resolving never rewrites the static job, so its paid results stand');
 await refresh.service.run({workspaceId:'design_test',jobId:freshFilm.jobId});
 const finished=await refresh.service.status({workspaceId:'design_test',...refresh.scope});
 await refresh.ref.collection('motionJobs').doc(freshFilm.jobId).update({originalSources:[{id:'pinned',asset:{path:'uncropped-original'}}]});
 const reframedRerun=await refresh.service.start({workspaceId:'design_test',...refresh.scope,rerunOf:freshFilm.jobId,repairReviewHash:finished.repairReviewHash});
 ok((await refresh.ref.collection('motionJobs').doc(reframedRerun.jobId).get()).data().originalSources[0].asset.path==='operator-crop','a re-run re-reads the framing instead of cloning a stale reference');
 const safe=await setup();
 await safe.ref.collection('editorAIJobs').doc(safe.id).collection('data').doc('request').set({sources:[{asset:{path:'photo'}}],identitySources:[{id:'pinned',asset:{path:'pinned-original'}}]});
 safe.D.identityFor=async()=>{throw Error('identity service unavailable')};
 const safeJob=await safe.service.start({workspaceId:'design_test',...safe.scope});
 ok((await safe.ref.collection('motionJobs').doc(safeJob.jobId).get()).data().originalSources[0].asset.path==='pinned-original','an unavailable resolver falls back to the pinned reference rather than stopping the film');

 // A workspace switched from one product to another keeps its editorAI pointer on the first product's design.
 // Animating the second product must use its own finished design, never fail with a scope error, and never borrow the other one.
 const bunny=await setup();
 await bunny.ref.set({settings:{productId:'b',groupRef:'g'}},{merge:true});
 bunny.D.context=async()=>({ref:bunny.ref,w:(await bunny.ref.get()).data(),products:[{id:'p',title:'Pendant',url:'https://example.test/p'},{id:'b',title:'Bunny',url:'https://example.test/b'}]});
 const bunnyScope={productId:'b',groupRef:'g'};let noDesign='';
 try{await bunny.service.start({workspaceId:'design_test',...bunnyScope});}catch(e){noDesign=e.message;}
 ok(/no saved design yet/.test(noDesign)&&!/belongs to another/.test(noDesign),'a product with no design of its own is told to run AI Design, not blamed on its scope ('+noDesign+')');
 ok((await bunny.ref.collection('motionJobs').get()).docs.length===0,'nothing is queued from the other product\'s design');
 const bunnyId='eai_'+'b'.repeat(40),bunnyDesign=bunny.ref.collection('editorAIJobs').doc(bunnyId);
 await bunnyDesign.set({id:bunnyId,scope:bunnyScope,phase:'ready',createdAt:5});await bunnyDesign.collection('data').doc('request').set({sources:[{asset:{path:'photo'}}]});await bunnyDesign.collection('data').doc('result').set({responsive:{plan:{nativeCopy:{headlines:['Bunny','A little luck']},layouts:[]}},sources:[{asset:{path:'photo'},width:100,height:100}]});
 const bunnyStart=await bunny.service.start({workspaceId:'design_test',...bunnyScope}),bunnyJob=(await bunny.ref.collection('motionJobs').doc(bunnyStart.jobId).get()).data();
 ok(bunnyJob.editorJobId===bunnyId&&bunnyJob.productId==='b'&&bunnyJob.title==='Bunny','the product\'s own finished design is animated even though the workspace pointer names another product\'s');

 // The Bunny's finished design may sit in an earlier version of the same ad, under another group of the same product, or still be awaiting review.
 const design=async(ref,id,scope,extra={})=>{const doc=ref.collection('editorAIJobs').doc(id);await doc.set({id,scope,phase:'ready',createdAt:7,...extra});await doc.collection('data').doc('request').set({sources:[{asset:{path:'photo'}}]});await doc.collection('data').doc('result').set({responsive:{plan:{nativeCopy:{headlines:['Bunny','A little luck']},layouts:[]}},sources:[{asset:{path:'photo'},width:100,height:100}]});};
 const products=[{id:'p',title:'Pendant',url:'https://example.test/p'},{id:'b',title:'Bunny',url:'https://example.test/b'}];
 const earlier=await setup(),earlierRef=earlier.D.fb().db.collection('Workspace').doc('design_earlier'),earlierId='eai_'+'c'.repeat(40);
 await earlier.ref.set({settings:bunnyScope,context:{campaignId:'c1',groups:[{ref:'g'}]}},{merge:true});await earlierRef.set({settings:bunnyScope,context:{campaignId:'c1',groups:[{ref:'g'}]}});await design(earlierRef,earlierId,bunnyScope,{nativeAppliedAt:9});
 earlier.D.context=async()=>({ref:earlier.ref,w:(await earlier.ref.get()).data(),products});earlier.D.relatedContexts=async()=>[{ref:earlierRef,w:(await earlierRef.get()).data()}];
 const earlierStart=await earlier.service.start({workspaceId:'design_test',...bunnyScope}),earlierJob=(await earlier.ref.collection('motionJobs').doc(earlierStart.jobId).get()).data();
 ok(earlierJob.editorJobId===earlierId&&earlierJob.editorWorkspaceId==='design_earlier'&&earlierJob.groupRef==='g','a finished design in an earlier version of the same ad is used, and the film stays in this workspace');
 const other=await setup();
 await other.ref.set({settings:bunnyScope,context:{groups:[{ref:'g'},{ref:'g2'}]}},{merge:true});other.D.context=async()=>({ref:other.ref,w:(await other.ref.get()).data(),products});
 await design(other.ref,'eai_'+'d'.repeat(40),{productId:'b',groupRef:'g2'});
 const otherStart=await other.service.start({workspaceId:'design_test',...bunnyScope}),otherJob=(await other.ref.collection('motionJobs').doc(otherStart.jobId).get()).data();
 ok(otherJob.editorJobId==='eai_'+'d'.repeat(40)&&otherJob.productId==='b'&&otherJob.groupRef==='g','the same product\'s design under another ad group is used for this group, never the other product\'s');
 const waiting=await setup();
 await waiting.ref.set({settings:bunnyScope},{merge:true});waiting.D.context=async()=>({ref:waiting.ref,w:(await waiting.ref.get()).data(),products});
 await waiting.ref.collection('editorAIJobs').doc('eai_'+'e'.repeat(40)).set({id:'eai_'+'e'.repeat(40),scope:bunnyScope,phase:'awaiting_review',createdAt:8});
 let notDone='';try{await waiting.service.start({workspaceId:'design_test',...bunnyScope});}catch(e){notDone=e.message;}
 ok(/not finished yet \(awaiting review\)/.test(notDone),'a design still awaiting review is named as unfinished ('+notDone+')');

 // No AI design anywhere, but the ad has a saved design: the film is made from that design's photos and the saved messaging, start to finish.
 const saved=await setup();
 await saved.ref.set({settings:bunnyScope,messaging:{productId:'b',groupRef:'g',copy:{headlines:['Bunny Pendant','A little luck'],longHeadlines:['A little luck to wear every single day'],descriptions:['Handmade bunny pendant necklace.']}}},{merge:true});
 saved.D.context=async()=>({ref:saved.ref,w:(await saved.ref.get()).data(),products});
 saved.D.motionBasis=async()=>({design:{id:'saved_design_1',name:'Bunny'},sources:[{id:'photo_1',asset:{path:'photo'},width:100,height:100}],originalSources:[]});
 const savedStart=await saved.service.start({workspaceId:'design_test',...bunnyScope}),savedJob=(await saved.ref.collection('motionJobs').doc(savedStart.jobId).get()).data();
 ok(savedJob.editorJobId===null&&savedJob.basisDesignId==='saved_design_1'&&savedJob.productId==='b'&&savedJob.plan.copy.shortHeadline==='Bunny Pendant'&&savedJob.plan.copy.description==='Handmade bunny pendant necklace.','a film is queued from the saved design and the saved messaging');
 const savedRun=await saved.service.run({workspaceId:'design_test',jobId:savedStart.jobId}),savedStatus=await saved.service.status({workspaceId:'design_test',...bunnyScope});
 ok(savedRun.ok&&savedStatus.phase==='ready'&&savedStatus.variants.length===3,'the film made from a saved design runs to three finished sizes ('+savedRun.error+')');

 // The film uses exactly the saved design's photos of this listing, in the static ads' order; another listing's photo is never used, at start or on a re-run.
 {
  const mine=(id,path)=>({id,asset:{path},width:100,height:100,source:{kind:'product',productId:'gid://shopify/Product/b',imageId:id}}),theirs={id:'photo_duck',asset:{path:'duck'},width:100,height:100,source:{kind:'product',productId:'other-duck',imageId:'x'}};
  const mixed=await setup();await mixed.ref.set({settings:bunnyScope},{merge:true});mixed.D.context=async()=>({ref:mixed.ref,w:(await mixed.ref.get()).data(),products});
  mixed.D.motionBasis=async()=>({design:{id:'saved_design_2',name:'Bunny'},sources:[theirs,mine('photo_a','bunny-a'),mine('photo_b','bunny-b')],originalSources:[theirs,mine('photo_a','bunny-a'),mine('photo_b','bunny-b')]});
  const mixedStart=await mixed.service.start({workspaceId:'design_test',...bunnyScope}),mixedJob=(await mixed.ref.collection('motionJobs').doc(mixedStart.jobId).get()).data();
  ok(mixedJob.originalSources.map(x=>x.id).join()==='photo_a,photo_b'&&mixedJob.sourceImages.map(x=>x.id).join()==='photo_a,photo_b','another listing\'s photo is dropped and the saved design\'s own photos keep their order');
  const alien=await setup();await alien.ref.set({settings:bunnyScope},{merge:true});alien.D.context=async()=>({ref:alien.ref,w:(await alien.ref.get()).data(),products});
  alien.D.motionBasis=async()=>({design:{id:'saved_design_3',name:'Bunny'},sources:[theirs],originalSources:[theirs]});
  await assert.rejects(()=>alien.service.start({workspaceId:'design_test',...bunnyScope}),/no photograph of its own listing/);n++;
  ok((await alien.ref.collection('motionJobs').get()).docs.length===0,'a design holding only another listing\'s photo queues no film');
  // A re-run re-validates a saved film that already holds the wrong photo.
  await mixed.service.run({workspaceId:'design_test',jobId:mixedJob.id});
  const mixedRef=mixed.ref.collection('motionJobs').doc(mixedJob.id);await mixedRef.update({originalSources:[theirs,mine('photo_a','bunny-a')],sourceImages:[theirs,mine('photo_a','bunny-a')]});
  const mixedState=await mixed.service.status({workspaceId:'design_test',...bunnyScope,jobId:mixedJob.id}),rerunMixed=await mixed.service.start({workspaceId:'design_test',...bunnyScope,rerunOf:mixedJob.id,repairReviewHash:mixedState.repairReviewHash}),rerunMixedJob=(await mixed.ref.collection('motionJobs').doc(rerunMixed.jobId).get()).data();
  ok(rerunMixedJob.originalSources.map(x=>x.id).join()==='photo_a'&&rerunMixedJob.sourceImages.map(x=>x.id).join()==='photo_a','a re-run drops another listing\'s photo from the saved film');
 }

  // One product per film: the prompt forbids a second piece, a photo of side-by-side pieces is cropped to one, and a review that sees two fails the set into the bounded repair.
  {
   const solo=require('../../netlify/functions/googleAdsSinglePiece'),motion=require('../../netlify/functions/googleAdsAdMotion');
   const shapes=async(boxes,bg='#ffffff')=>sharp({create:{width:600,height:300,channels:3,background:bg}}).composite(boxes.map(([left,top,width,height])=>({input:{create:{width,height,channels:3,background:'#8a6a1f'}},left,top}))).jpeg().toBuffer();
   const pair=await solo.isolate(await shapes([[60,70,140,160],[390,70,140,160]])),one=await solo.isolate(await shapes([[220,70,160,160]])),busy=await solo.isolate(await shapes([[60,70,140,160],[390,70,140,160]],'#b69b74').then(b=>sharp(b).composite([{input:{create:{width:600,height:40,channels:3,background:'#203040'}},left:0,top:0}]).jpeg().toBuffer()));
   ok(pair&&pair.pieces===2&&(await sharp(pair.buffer).metadata()).width<400,'a photograph of two pieces side by side is cropped to one');
   ok(one===null&&busy===null,'a single piece, or a scene with no plain background, is sent as it is');
   const prompt=motion.motionPrompt({title:'Pendant',plan:{},creativeDirection:{setting:'a sunlit riverbank'}},'square','');
   ok(/ONE PIECE ONLY/.test(prompt)&&/Never two, never a pair, never a set, never side by side/.test(prompt)&&/No grid, no collage, no split frame/.test(prompt)&&/area beyond its middle holds none either/.test(prompt),'every film prompt allows exactly one piece, including beyond the square crop');
   const two=await setup();two.D.reviewImages=async()=>({pass:true,productFaithful:true,mobileReadable:true,score:98,issues:[],multipleProducts:true});
   const st=await two.service.start({workspaceId:'design_test',...two.scope});await two.service.run({workspaceId:'design_test',jobId:st.jobId});const state=await two.service.status({workspaceId:'design_test',...two.scope});
   ok(state.quality.pass===false&&!state.qualityTargetMet&&state.canRepair&&state.quality.issues.some(i=>/more than one piece/.test(i)),'a review that sees two pieces fails a high-scoring set and offers the bounded repair');
  }
  // A reviewed defect in the square format regenerates only the square master.
  {
   const sf=await setup();sf.D.reviewImages=async()=>{sf.calls.review++;return {pass:false,productFaithful:true,mobileReadable:true,score:85,scores:{messaging:100,layout:100,relevance:100,visualAppeal:85,productRecognition:100},categoryReviews:{messaging:{summary:'',deductions:[]},layout:{summary:'',deductions:[]},visualAppeal:{summary:'',deductions:[{points:15,reason:'Flat lighting',evidence:'mobile_square at 3.5s',correction:'Add a travelling reflection across the metal',kind:'required',formats:['mobile_square']}]},relevance:{summary:'',deductions:[]},productRecognition:{summary:'',deductions:[]}},issues:[]};};
   const st=await sf.service.start({workspaceId:'design_test',...sf.scope});await sf.service.run({workspaceId:'design_test',jobId:st.jobId});const state=await sf.service.status({workspaceId:'design_test',...sf.scope});
   const option=state.fixOptions[0];ok(state.fixOptions.length===1&&option.kind==='master'&&option.orientation==='square'&&option.formats.join()==='mobile_square'&&/only the square film/.test(option.label)&&/other films kept/.test(option.label)&&option.estimatedUsd>1&&option.estimatedUsd<1.1,'a square finding offers to regenerate only the square film');
   const fixed=await sf.service.start({workspaceId:'design_test',...sf.scope,fixOf:st.jobId,repairReviewHash:state.repairReviewHash,fix:{category:'visualAppeal',index:0}}),parent=(await sf.ref.collection('motionJobs').doc(st.jobId).get()).data(),child=(await sf.ref.collection('motionJobs').doc(fixed.jobId).get()).data(),before=sf.calls.create;
   ok(child.pipelineVersion===3&&child.renderVersion===11&&Object.keys(child.masters).join()==='portrait,landscape'&&child.variants.map(v=>v.key).sort().join()==='desktop_landscape,mobile_portrait','the fix keeps the portrait and landscape films and drops only the square film');
   ok((await sf.service.run({workspaceId:'design_test',jobId:fixed.jobId})).ok&&sf.calls.create===before+1&&sf.calls.aspects.slice(-1)[0]==='16:9','the fix buys one 16:9 master');
   const done=(await sf.ref.collection('motionJobs').doc(fixed.jobId).get()).data();ok(Object.keys(done.masters).join()==='portrait,landscape,square'&&done.masters.portrait.id===parent.masters.portrait.id&&done.masters.landscape.id===parent.masters.landscape.id&&done.masters.square.id!==parent.masters.square.id&&done.variants.length===3&&done.variants.find(v=>v.key==='mobile_square').master==='square','only the square master and its format are new');
   const fixes=require('../../netlify/functions/googleAdsAdFixes'),deduction={category:'visualAppeal',index:0,reason:'Flat lighting',evidence:'the film',correction:'Add a travelling reflection across the metal'},keys=['mobile_portrait','mobile_square','desktop_landscape'];
   ok(fixes.classifyAnimated(deduction,{formatKeys:keys,squareMaster:'square',masterUsd:1}).orientation==='portrait'&&fixes.masterFor('mobile_square','square')==='square'&&fixes.masterFor('mobile_square','landscape')==='landscape'&&fixes.masterFor('mobile_square')==='portrait','master choice: version 3 square is its own film, older jobs still borrow one');
  }
  // The square master is measured over the full frame; its format is the straight centre crop.
  {
   const {geometry,defaultBounds,resolveBounds,layoutRequest}=require('../../netlify/functions/googleAdsMotionComposition'),SQ={key:'square',width:720,height:720},inside={x:.42,y:.3,w:.3,h:.6};
   const g=geometry(SQ,inside,'square',{mode:'full'});ok(g.mode==='full'&&g.crop.x===.21875&&g.crop.y===0&&g.crop.w===.5625&&g.crop.h===1,'the square format crops exactly the middle 720x720 of the wide master');
   ok(Math.abs(g.product.x-(inside.x-.21875)/.5625)<1e-9&&Math.abs(g.product.w-inside.w/.5625)<1e-9&&g.product.y===inside.y&&g.product.h===inside.h,'the jewelry is placed inside the crop without any zoom');
   assert.throws(()=>geometry(SQ,{x:.15,y:.3,w:.3,h:.6},'square',{mode:'full'}),/not inside the central square/);n++;assert.throws(()=>geometry(SQ,{x:.6,y:.3,w:.25,h:.6},'square',{mode:'full'}),/not inside the central square/);n++;
   const band=geometry(SQ,{x:.15,y:.3,w:.3,h:.6},'square',{mode:'band'});ok(band.mode==='band'&&band.product.x>=-.002&&band.product.x+band.product.w<=1.002,'a jewelry outside the central square still gets the band fallback');
   const d=defaultBounds('square');ok(d.x>=.21875&&d.x+d.w<=.78125,'the directed region for the square master lies inside the central square');
   const resolved=resolveBounds({square:{bounds:[.45,.3,.28,.55],complete:true,confidence:.99,note:'fixture'}},['square']);ok(resolved.bounds.square.x>=.21875&&resolved.bounds.square.x+resolved.bounds.square.w<=.78125&&!resolved.notes.length,'a measured square master keeps its jewelry inside the central square');
   ok(/FULL wide frame/.test(layoutRequest({title:'x'},[],Buffer.from('i'),['portrait','square','landscape']).input[0].content)&&!/FULL wide frame/.test(layoutRequest({title:'x'},[],Buffer.from('i')).input[0].content),'the layout request asks for full-frame bounds only when it measures a square master');
  }
 win.close();console.log('PASS '+n+' Gemini request, durable generation, device variants and recovery checks');
 require('./suite-guard.cjs').done();
})().catch(e=>{console.error(e.stack);process.exitCode=1});

