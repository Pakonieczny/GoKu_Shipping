const assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm'),sharp=require('sharp');
const {requestBody,outputVideo,createGeminiVideo,MODEL}=require('../../netlify/functions/googleAdsGeminiVideo');
const {createMotionService,renderVariants}=require('../../netlify/functions/googleAdsAdMotion');
const fixture=path.join(__dirname,'ad-design-workflow.cjs'),source=fs.readFileSync(fixture,'utf8').split('(async()=>{const e=await setup();')[0];
const ctx=vm.createContext({require:require('node:module').createRequire(fixture),__dirname,process,Buffer,console,Date,setTimeout,clearTimeout});vm.runInContext(source+'\nglobalThis.mem=memory;',ctx);
let n=0;function ok(v,m){assert(v,m);n++;}
// Every progress label a job saves, in order: what the panel would have shown while the job ran.
function watchLabels(x){const labels=[],run=x.f.db.runTransaction;x.f.db.runTransaction=fn=>run(tx=>fn({...tx,update:(r,v)=>{const l=v?.progress?.label;if(l&&labels[labels.length-1]!==l)labels.push(l);return tx.update(r,v);}}));return labels;}
// A provider whose films are still in progress when requested and complete on the next poll, so the run reaches its "Generating motion" stage.
function slowFilms(x,onPoll){const inner=x.D.videoRequest;x.D.videoRequest=async(route,method,body)=>{if(method==='POST'){const r=await inner(route,method,body);return {id:r.id,status:'in_progress'};}if(onPoll)await onPoll();return {id:route.split('/').pop(),status:'completed',steps:[{type:'model_output',content:[{type:'video',uri:'https://storage.googleapis.com/video.mp4'}]}]};};}
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
 let sent=0;const provider=createGeminiVideo({apiKey:'fixture',fetch:async(url,opts)=>{sent++;ok(opts.headers['x-goog-api-key']==='fixture'&&!url.includes('fixture'),'key kept in header');return {ok:true,status:200,text:async()=>'{"id":"v1_1"}',headers:{get:()=>null}}}});await provider.request('interactions','POST',body);await assert.rejects(()=>provider.content({uri:'https://attacker.test/video'}),/unsupported/);ok(sent===1,'no credentials or fetch to untrusted download host');
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
   const p3Labels=watchLabels(p3);
   ok((await p3.service.run({workspaceId:'design_test',jobId:ps.jobId})).ok&&p3.calls.create===1&&p3.calls.aspects.join()==='16:9'&&polls.length===1,'the pending square master is polled and only the missing landscape master is requested');
   ok(p3Labels.includes('Generating motion · square and landscape')&&!p3Labels.some(l=>/portrait, square and landscape/.test(l)),'a resume with the portrait master already done names only the two films still being made ('+p3Labels.join(' | ')+')');
  }
  {
   // Google answering a poll with an event stream ("event: error"), a gateway page or a reset never fails or re-buys a paid film: the same
   // saved interaction is asked again, the job stays resumable, and the panel gets a plain sentence with the technical reason kept apart.
   const reply=(status,text,headers={})=>({ok:status>=200&&status<300,status,headers:{get:k=>headers[k.toLowerCase()]||null},text:async()=>text,buffer:async()=>Buffer.from(text)});
   const stream='event: error\ndata: {"error":{"message":"Internal error encountered."}}\n\n',finished=JSON.stringify({id:'v1_pending',status:'completed',steps:[{type:'model_output',content:[{type:'video',uri:'https://storage.googleapis.com/pending.mp4'}]}]});
   const film=(id,status,size)=>({id,status,progress:status==='completed'?100:0,output:status==='completed'?{uri:'https://storage.googleapis.com/saved.mp4'}:null,size,requestId:'saved',estimatedUsd:1.0136});
   const seed=async()=>{const x=await setup(),s=await x.service.start({workspaceId:'design_test',...x.scope}),r=x.ref.collection('motionJobs').doc(s.jobId);
    await r.update({masters:{portrait:film('v1_p','completed','720x1280'),square:film('v1_pending','in_progress','1280x720'),landscape:film('v1_l','completed','1280x720')}});return {x,r,id:s.jobId};};
   const wire=(x,answer)=>{const urls=[],sleeps=[],g=createGeminiVideo({apiKey:'fixture',sleep:async ms=>{sleeps.push(ms);},fetch:async(url,opts)=>{urls.push(opts.method+' '+url);return answer(urls.length);}});x.D.videoRequest=g.request;x.D.sleep=async()=>{};return {urls,sleeps};};
   const POLL='GET https://generativelanguage.googleapis.com/v1beta/interactions/v1_pending';
   // Two unreadable replies, then the finished film: one interaction, polled three times, no second purchase.
   const a=await seed(),wa=wire(a.x,n=>n<=2?reply(200,stream):reply(200,finished));
   ok((await a.x.service.run({workspaceId:'design_test',jobId:a.id})).ok,'a film survives two unreadable event-stream replies');
   const aj=(await a.r.get()).data();
   ok(wa.urls.length===3&&wa.urls.every(u=>u===POLL)&&a.x.calls.create===0,'the same saved interaction is polled again and no second film is requested');
   ok(aj.phase==='ready'&&aj.variants.length===3&&aj.masters.square.id==='v1_pending'&&aj.masters.square.status==='completed'&&!aj.error,'the job ends ready with the original interaction');
   ok(wa.sleeps.length===2&&wa.sleeps[0]>0,'the provider backed off between tries');
   // Google keeps answering unreadably: the job stops resumable (never failed), with a plain message, and resuming polls the same film.
   const b=await seed(),wb=wire(b.x,()=>reply(200,stream)),out=await b.x.service.run({workspaceId:'design_test',jobId:b.id}),bj=(await b.r.get()).data();
   ok(out.ok===false&&bj.phase==='needs_attention'&&bj.phase!=='failed'&&bj.masters.square.id==='v1_pending'&&bj.masters.portrait.id==='v1_p'&&!bj.inFlight,'persistent unreadable replies leave the job resumable with every interaction id kept');
   ok(/Google’s video service/.test(bj.error)&&/Check saved progress/.test(bj.error)&&!/Unexpected token|not valid JSON|event: err/.test(bj.error),'the panel message is plain, not a parse error');
   ok(/event stream/.test(bj.errorDetail)&&/Internal error encountered/.test(bj.errorDetail),'the technical reason is kept in the details');
   ok(wb.urls.length>3&&wb.urls.length<=12&&wb.urls.every(u=>u===POLL)&&b.x.calls.create===0,'retries are bounded and only ever ask for the saved interaction');
   const bs=await b.x.service.status({workspaceId:'design_test',...b.x.scope});ok(bs.canResume&&bs.providerWait&&bs.phase==='needs_attention'&&/Waiting for Google/.test(bs.progress.label)&&/Internal error/.test(bs.errorDetail),'status offers resume by polling and carries the details');
   const wc=wire(b.x,()=>reply(200,finished));await b.x.service.start({workspaceId:'design_test',...b.x.scope,resumeJobId:b.id});
   ok((await b.x.service.run({workspaceId:'design_test',jobId:b.id})).ok&&wc.urls.length===1&&wc.urls[0]===POLL&&b.x.calls.create===0,'Resume animation polls the same interaction and buys nothing');
   const bj2=(await b.r.get()).data();ok(bj2.phase==='ready'&&!bj2.error&&!bj2.errorDetail&&!bj2.providerWait&&bj2.variants.length===3,'the resumed film completes and clears the wait state');
   // Real refusals stay terminal with Google's own words; the same reading covers create, event-stream results and the finished-file download.
   const f=answer=>createGeminiVideo({apiKey:'fixture',sleep:async()=>{},fetch:async()=>answer()});
   await assert.rejects(()=>f(()=>reply(404,'{"error":{"code":404,"message":"Interaction not found","status":"NOT_FOUND"}}')).request('interactions/v1_x'),e=>e.transient===false&&/Interaction not found/.test(e.message)&&e.definiteResponse===true);n++;
   await assert.rejects(()=>f(()=>reply(200,'event: error\ndata: {"error":{"code":400,"message":"Blocked by safety filters"}}\n\n')).request('interactions/v1_x'),e=>e.transient===false&&/safety/.test(e.message));n++;
   ok((await f(()=>reply(200,'event: interaction.start\ndata: {"interaction":{"id":"v1_x","status":"in_progress"}}\n\nevent: interaction.complete\ndata: {"event_type":"interaction.complete","interaction":{"id":"v1_x","status":"completed"}}\n\n')).request('interactions/v1_x')).status==='completed','an interaction.complete event carries the result');
   ok((await f(()=>reply(200,'event: interaction.start\ndata: {"interaction":{"id":"v1_new","status":"in_progress"}}\n\n'+stream)).request('interactions','POST',{})).id==='v1_new','a create whose stream already named the interaction keeps its id');
   await assert.rejects(()=>f(()=>reply(200,stream)).request('interactions','POST',{}),e=>e.transient===true&&e.definiteResponse===false&&/nothing is requested again/.test(e.message));n++;
   let tries=0;const dl=createGeminiVideo({apiKey:'fixture',sleep:async()=>{},fetch:async()=>++tries===1?reply(200,stream,{'content-type':'text/event-stream'}):reply(200,'MP4',{'content-type':'video/mp4'})});
   ok((await dl.content({uri:'https://storage.googleapis.com/v.mp4'})).toString()==='MP4'&&tries===2,'an event stream in place of the video is retried, never saved as a film');
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
 {
  // An explicit Re-run is always a NEW job: it never lands on the finished (or discarded) job it re-runs, the previous job and its films stay
  // exactly as saved, the same press shares one new job, and the panel follows the new id. Only a plain Generate meets the two-master legacy guard.
  const rr=await setup(),scopeRR={workspaceId:'design_test',...rr.scope},jobsRR=rr.ref.collection('motionJobs'),rowRR=async id=>(await jobsRR.doc(id).get()).data();
  const first=await rr.service.start(scopeRR);await rr.service.run({workspaceId:'design_test',jobId:first.jobId});
  const firstState=await rr.service.status(scopeRR),firstSaved=JSON.stringify(await rowRR(first.jobId));
  ok(firstState.phase==='ready'&&firstState.variants.length===3&&firstState.repairReviewHash&&rr.calls.create===3,'a finished job with three films is the starting point of a re-run');
  const press=key=>({...scopeRR,rerunOf:first.jobId,repairReviewHash:firstState.repairReviewHash,...(key?{rerunKey:key}:{})});
  const one=await rr.service.start(press('press-one-aaaa'));
  ok(one.jobId!==first.jobId&&one.queued&&one.attempt===1&&/^motion_[a-f0-9]{40}$/.test(one.jobId),'a re-run gets its own new job id');
  const oneRow=await rowRR(one.jobId);ok(oneRow.phase==='queued'&&oneRow.rerunAttempt===1&&oneRow.rerunKey==='press-one-aaaa'&&oneRow.repairOf===first.jobId&&!Object.keys(oneRow.masters).length&&!oneRow.variants.length&&!oneRow.quality&&oneRow.pipelineVersion===3&&oneRow.renderVersion===11&&oneRow.sourceImages.length&&oneRow.originalSources.length,'the new job is queued to make all three films again from the saved design photographs');
  ok(JSON.stringify(await rowRR(first.jobId))===firstSaved,'the previous job and its films are untouched by the re-run');
  ok((await rr.service.start(press('press-one-aaaa'))).jobId===one.jobId&&(await rr.service.start(press())).jobId===one.jobId,'the same press, or a keyless repeat within seconds, shares one new job');
  await assert.rejects(()=>rr.service.start(press('press-two-bbbb')),/already being made and paid for/);n++;
  ok((await jobsRR.get()).docs.length===2&&rr.calls.create===3,'a repeat neither creates a second job nor buys anything');
  const following=await rr.service.status({...scopeRR,jobId:one.jobId});ok(following.jobId===one.jobId&&following.phase==='queued'&&!following.variants.length&&!following.quality&&following.repairOf===first.jobId,'the new job reports as queued with none of the previous films or review');
  ok((await rr.service.status(scopeRR)).jobId===one.jobId,'the next status of the panel is the new job, not the finished one');
  {
   // The panel: pressing Re-run sends a fresh request key, switches to the new job at once (no old films or review shown as done) and polls that job by id.
   const {JSDOM}=require('jsdom'),dom=new JSDOM('<div id="motion"></div>',{runScripts:'outside-only',url:'https://example.test'}),w=dom.window,sent=[];w.setTimeout=()=>0;w.eval(fs.readFileSync(path.join(__dirname,'../../brites-ad-motion.js'),'utf8'));
   w.BritesAdMotion.mount(w.document.getElementById('motion'),{scope:scopeRR,request:async(action,payload)=>{sent.push({action,payload});return action==='startAdDesignMotion'?{ok:true,workspaceId:'design_test',jobId:one.jobId,queued:true}:payload.jobId?following:firstState;}});
   await new Promise(resolve=>setImmediate(resolve));ok(w.document.querySelector('video')&&w.document.querySelector('[data-generate]').textContent==='Re-run animation','the panel first shows the finished films');
   w.document.querySelector('[data-generate]').click();await [...w.document.querySelectorAll('[data-confirm] button')].find(b=>b.textContent==='Re-run animation').onclick();
   const asked=sent.find(r=>r.action==='startAdDesignMotion').payload,polled=sent.filter(r=>r.action==='adDesignMotionStatus').slice(-1)[0].payload;
   ok(asked.rerunOf===first.jobId&&/^[A-Za-z0-9_-]{8,64}$/.test(asked.rerunKey)&&polled.jobId===one.jobId,'the panel sends a request key with the re-run and then polls the new job by id');
   ok(!w.document.querySelector('video')&&!w.document.querySelector('[data-quality]').textContent&&/Queued|Preparing/.test(w.document.querySelector('[data-label]').textContent)&&w.document.querySelector('[data-generate]').disabled,'after Re-run the panel shows the new job in progress, not the previous films as done');
   w.close();
  }
  // The reported case: the attempt is discarded, then Re-run is pressed again on the same finished job.
  await rr.service.start({...scopeRR,discardJobId:one.jobId,confirmDiscard:true});
  ok((await rr.service.status(scopeRR)).jobId===first.jobId,'after a discard the finished job is what the panel shows');
  const two=await rr.service.start(press('press-two-bbbb'));
  ok(two.jobId!==first.jobId&&two.jobId!==one.jobId&&two.queued&&two.attempt===2,'Re-run after a discarded attempt is a third job, never the finished or the discarded one');
  const afterDiscard=await rr.service.status(scopeRR);ok(afterDiscard.jobId===two.jobId&&afterDiscard.phase==='queued'&&!afterDiscard.variants.length,'the panel now shows the new job in progress, not the old films as done');
  ok((await rowRR(one.jobId)).resetAt&&JSON.stringify(await rowRR(first.jobId))===firstSaved,'the discarded attempt and the finished job remain saved');
  ok((await rr.service.start(press())).jobId===two.jobId&&(await jobsRR.get()).docs.length===3,'a repeat of that press still shares its one job');
  await rr.service.run({workspaceId:'design_test',jobId:two.jobId});const twoRow=await rowRR(two.jobId),firstIds=Object.values((await rowRR(first.jobId)).masters).map(m=>m.id);
  ok(rr.calls.create===6&&twoRow.phase==='ready'&&twoRow.variants.length===3&&Object.values(twoRow.masters).length===3&&Object.values(twoRow.masters).every(m=>!firstIds.includes(m.id)),'the new job buys three new films with provider ids of its own');
  // A two-master film saved for the same design answers a plain Generate, never a re-run.
  const legacyId='motion_'+require('node:crypto').createHash('sha256').update('eai_'+'a'.repeat(40)+':v2:after:'+one.jobId).digest('hex').slice(0,40);
  await jobsRR.doc(legacyId).set({...(await rowRR(first.jobId)),id:legacyId,pipelineVersion:2,createdAt:1});
  ok((await rr.service.start(scopeRR)).jobId===legacyId,'a plain Generate still returns the saved two-master film for the design');
  const twoState=await rr.service.status({...scopeRR,jobId:two.jobId}),three=await rr.service.start({...scopeRR,rerunOf:two.jobId,repairReviewHash:twoState.repairReviewHash,rerunKey:'press-three-cccc'}),four=await rr.service.start(press('press-four-dddd'));
  ok(three.queued&&four.queued&&new Set([first.jobId,one.jobId,two.jobId,three.jobId,four.jobId,legacyId]).size===6&&four.attempt===3,'the legacy guard does not swallow a re-run of a version 3 job, from the newest job or from an older one');
 }
 // Targeted fixes from the review: only the named film, captions or message change.
 const fx=await setup(),fxPlan=fx.D.planMotion;let copyFixes=0;fx.D.planMotion=async r=>r.text?.format?.name==='brites_motion_copy_fix'?(copyFixes++,{output_text:JSON.stringify({headline:'For the foodie who has everything',shortHeadline:'Pendant',description:'Gift-ready',cta:'Shop now',rationale:'specific hook'})}):fxPlan(r);
 fx.D.reviewImages=async(source,files,brief)=>{fx.calls.review++;fx.lastBrief=brief;return {pass:false,productFaithful:true,mobileReadable:true,score:85,scores:{messaging:90,layout:85,relevance:100,visualAppeal:90,productRecognition:100},categoryReviews:{messaging:{summary:'',deductions:[{points:10,reason:'Generic hook',evidence:'mobile_portrait at 0.3s',correction:'Rewrite the opening line as a specific gift hook',kind:'required',formats:['mobile_portrait']}]},layout:{summary:'',deductions:[{points:15,reason:'Caption too small',evidence:'mobile_square at 3.5s',correction:'Enlarge the caption',kind:'required',formats:['mobile_square']}]},visualAppeal:{summary:'',deductions:[{points:10,reason:'Flat lighting',evidence:'desktop_landscape frames',correction:'Add a travelling reflection across the metal',kind:'optional',formats:['desktop_landscape']}]},relevance:{summary:'',deductions:[]},productRecognition:{summary:'',deductions:[]}},issues:[]};};
 const fxStart=await fx.service.start({workspaceId:'design_test',...fx.scope});await fx.service.run({workspaceId:'design_test',jobId:fxStart.jobId});const fxState=await fx.service.status({workspaceId:'design_test',...fx.scope});
 ok(fxState.phase==='ready'&&fxState.canFix&&fxState.fixOptions.length===3,'every reviewed deduction offers one targeted fix');
 const byKind=Object.fromEntries(fxState.fixOptions.map(o=>[o.kind,o]));ok(byKind.copy?.category==='messaging'&&byKind.caption?.formats.join()==='mobile_square'&&byKind.master?.orientation==='landscape'&&byKind.master.estimatedUsd>1&&byKind.master.estimatedUsd<1.1,'copy, caption and single-film fixes are classified with honest scope and cost');
 await assert.rejects(()=>fx.service.start({workspaceId:'design_test',...fx.scope,fixOf:fxStart.jobId,repairReviewHash:'stale',fix:{category:'layout',index:0}}),/review changed/);n++;
 const captionFix={workspaceId:'design_test',...fx.scope,fixOf:fxStart.jobId,repairReviewHash:fxState.repairReviewHash,fix:{category:'layout',index:0}},cf=await fx.service.start(captionFix);
 ok(cf.queued&&(await fx.service.start(captionFix)).jobId===cf.jobId&&cf.jobId!==fxStart.jobId,'caption fix is a distinct, idempotent job');
 {const q=await fx.service.status({workspaceId:'design_test',...fx.scope,jobId:cf.jobId});ok(cf.making.join()==='square'&&q.making.join()==='square'&&/ · square$/.test(q.progress.label)&&!/portrait|landscape/.test(q.progress.label),'a caption fix of one size is queued as making that size only ('+q.progress.label+')');}
 const beforeCaption={...fx.calls};ok((await fx.service.run({workspaceId:'design_test',jobId:cf.jobId})).ok,'caption fix completes');
 const cfState=await fx.service.status({workspaceId:'design_test',...fx.scope,jobId:cf.jobId});
 ok(cfState.phase==='ready'&&cfState.fixOf===fxStart.jobId&&cfState.variants.length===3&&fx.calls.create===beforeCaption.create&&fx.calls.download===beforeCaption.download&&fx.calls.review===beforeCaption.review+1,'caption fix re-composes without any new film or download and re-reviews the set');
 ok(cfState.variants.filter(v=>v.key!=='mobile_square').every(v=>fxState.variants.some(o=>o.key===v.key&&o.asset.hash===v.asset.hash)),'unaffected films keep their exact saved bytes');
 ok(fx.lastBrief.targetedFix?.formats.join()==='mobile_square'&&fx.lastBrief.fixNote,'the review is told which format was corrected');
 ok(cfState.estimatedUsd===0&&(await fx.ref.collection('motionJobs').doc(cf.jobId).get()).data().captionHints.square.preferBand,'caption fix costs nothing and records its hint');
 const masterFix={workspaceId:'design_test',...fx.scope,fixOf:fxStart.jobId,repairReviewHash:fxState.repairReviewHash,fix:{category:'visualAppeal',index:0}},mf=await fx.service.start(masterFix),beforeMaster={...fx.calls};
 {const q=await fx.service.status({workspaceId:'design_test',...fx.scope,jobId:mf.jobId});ok(mf.making.join()==='landscape'&&q.making.join()==='landscape'&&!/portrait|square/.test(q.progress.label),'a fix that regenerates one film is queued as making that size only ('+q.progress.label+')');}
 ok((await fx.service.run({workspaceId:'design_test',jobId:mf.jobId})).ok,'master fix completes');const mfState=await fx.service.status({workspaceId:'design_test',...fx.scope,jobId:mf.jobId});
 ok(fx.calls.create===beforeMaster.create+1&&mfState.variants.length===3&&mfState.variants.find(v=>v.key==='mobile_portrait').asset.hash===fxState.variants.find(v=>v.key==='mobile_portrait').asset.hash,'master fix regenerates exactly one film and keeps the portrait film');
 ok(Math.abs(mfState.estimatedUsd-1.015)<.01,'master fix reports one film of video cost');
 const copyFix={workspaceId:'design_test',...fx.scope,fixOf:fxStart.jobId,repairReviewHash:fxState.repairReviewHash,fix:{category:'messaging',index:0}},cpf=await fx.service.start(copyFix),beforeCopy={...fx.calls};
 ok((await fx.service.status({workspaceId:'design_test',...fx.scope,jobId:cpf.jobId})).making.join()==='portrait,square,landscape','a messaging fix re-composes every size, so it says all three');
 ok((await fx.service.run({workspaceId:'design_test',jobId:cpf.jobId})).ok,'copy fix completes');const cpfState=await fx.service.status({workspaceId:'design_test',...fx.scope,jobId:cpf.jobId}),cpfJob=(await fx.ref.collection('motionJobs').doc(cpf.jobId).get()).data();
 ok(copyFixes===1&&fx.calls.create===beforeCopy.create&&cpfState.variants.length===3&&cpfJob.plan.copy.headline==='For the foodie who has everything'&&cpfJob.copyFixed,'copy fix revises the message once and re-composes every format without new video');
 await fx.service.run({workspaceId:'design_test',jobId:cpf.jobId});ok(copyFixes===1&&fx.calls.create===beforeCopy.create,'completed fixes never charge twice');
 ok((await fx.service.status({workspaceId:'design_test',...fx.scope})).jobId===cpf.jobId,'the latest fix becomes the current animation');
 // Progress wording follows what the job will really make: a full run keeps its three-size wording all the way through.
 {
  const fr=await setup(),W={workspaceId:'design_test'},labels=watchLabels(fr),mids=[];
  const first=await fr.service.start({...W,...fr.scope});
  ok((await fr.service.status({...W,...fr.scope})).making.join()==='portrait,square,landscape','a new job is making all three sizes');
  const inner=fr.D.renderVariants;fr.D.renderVariants=async(...a)=>{mids.push((await fr.service.status({...W,...fr.scope,jobId:first.jobId})).making.join());return inner(...a);};
  slowFilms(fr);await fr.service.run({...W,jobId:first.jobId});
  ok(labels.includes('Generating motion · portrait, square and landscape')&&labels.includes('Saved 3 of 3 video formats')&&labels.at(-1)==='Three video formats ready to preview','a full run reads exactly as before ('+labels.join(' | ')+')');
  ok(mids.length===3&&mids.every(m=>m==='portrait,square,landscape'),'saving one video never shrinks the list of sizes a running job is making');
 }
 // Redo one film on demand: any format of a finished job, exactly one new master, every other film kept as saved.
 {
  const rd=await setup(),W={workspaceId:'design_test'},first=await rd.service.start({...W,...rd.scope});await rd.service.run({...W,jobId:first.jobId});
  const rdState=await rd.service.status({...W,...rd.scope}),parentRef=rd.ref.collection('motionJobs').doc(first.jobId),parentBefore=JSON.stringify((await parentRef.get()).data());
  ok(rdState.phase==='ready'&&rdState.variants.length===3&&rdState.redoOptions.map(o=>o.format).join()==='portrait,square,landscape'&&rdState.redoOptions.every(o=>o.hasFilm),'a ready job offers a redo for each of its three films');
  const sq=rdState.redoOptions.find(o=>o.format==='square');
  ok(sq.formats.join()==='mobile_square'&&sq.kept.join()==='mobile_portrait,desktop_landscape'&&Math.abs(sq.estimatedUsd*3-rdState.estimatedUsd)<.02&&sq.estimatedUsd>1&&sq.estimatedUsd<1.1,'redoing one film costs a third of the set (one master) and keeps the other two');
  const input={...W,...rd.scope,redoOf:first.jobId,redoFormat:'square',confirmRedo:true},create0=rd.calls.create,bodies0=rd.calls.bodies.length,renders0=rd.calls.renders.length,review0=rd.calls.review,download0=rd.calls.download;
  await assert.rejects(()=>rd.service.start({...input,confirmRedo:false}),/Confirm the video to redo/);n++;
  await assert.rejects(()=>rd.service.start({...input,redoFormat:'widescreen'}),/Choose the portrait, square or landscape/);n++;
  const started=await rd.service.start(input);
  ok(started.queued&&started.jobId!==first.jobId&&(await rd.service.start(input)).jobId===started.jobId&&rd.calls.create===create0,'the redo is a distinct, idempotent job and starting it buys nothing');
  // A one-video redo makes one size: the start reply, the queued status, every progress label while it runs and the finished label name only the square.
  const queued=await rd.service.status({...W,...rd.scope,jobId:started.jobId}),redoLabels=watchLabels(rd),running=[];
  ok(started.making.join()==='square'&&(await rd.service.start(input)).making.join()==='square'&&queued.phase==='queued'&&queued.making.join()==='square'&&queued.progress.label==='Preparing one new square video','a one-video redo is queued as making the square only');
  slowFilms(rd,async()=>{const s=await rd.service.status({...W,...rd.scope,jobId:started.jobId});running.push(s.phase+':'+s.making.join());});
  ok((await rd.service.run({...W,jobId:started.jobId})).ok,'the redo completes');
  ok(running.join()==='running:square','while the redo runs its status still says it is making the square only');
  ok(redoLabels.includes('Generating motion · square')&&redoLabels.includes('Saved 1 of 1 new video format')&&redoLabels.at(-1)==='Square video ready to preview · all 3 sizes saved'&&!redoLabels.some(l=>/portrait|landscape|three|Saved 3|Saved 2/i.test(l.replace(/all 3 sizes saved/,''))),'no progress label of a one-video redo names the other formats or promises three ('+redoLabels.join(' | ')+')');
  const done=await rd.service.status({...W,...rd.scope}),job=(await rd.ref.collection('motionJobs').doc(started.jobId).get()).data();
  ok(rd.calls.create===create0+1&&rd.calls.aspects.slice(3).join()==='16:9'&&rd.calls.download===download0+1,'exactly one new master is bought and downloaded');
  ok(rd.calls.renders.length===renders0+1&&rd.calls.renders[renders0].orientation==='square'&&rd.calls.renders[renders0].plan.renderVersion===11&&rd.calls.renders[renders0].plan.composition.w>0&&rd.calls.review===review0+1,'only the square film is composed again, with the same render version and its own measured framing, then the set is reviewed');
  ok(rd.calls.bodies[bodies0].input[0].data===rd.calls.bodies[0].input[0].data&&rd.calls.bodies[bodies0].input[1].text===rd.calls.bodies[1].input[1].text,'the new film is made from the same reference photograph and the same square prompt');
  const byKey=(s,k)=>s.variants.find(v=>v.key===k);
  ok(done.jobId===started.jobId&&done.redoOf===first.jobId&&done.phase==='ready'&&done.variants.length===3&&['mobile_portrait','desktop_landscape'].every(k=>JSON.stringify(byKey(done,k))===JSON.stringify(byKey(rdState,k))),'the other two videos are unchanged byte for byte');
  ok(byKey(done,'mobile_square').asset.hash!==byKey(rdState,'mobile_square').asset.hash&&byKey(done,'mobile_square').master==='square'&&job.masters.square.id==='v1_'+(create0+1)&&job.masters.portrait.id===JSON.parse(parentBefore).masters.portrait.id&&job.masters.landscape.id===JSON.parse(parentBefore).masters.landscape.id,'the new square film replaces the old one as the current version and the other masters are the saved ones');
  ok(Math.abs(done.estimatedUsd-1.0136)<.02&&done.fixTarget.category==='redo'&&done.fixOf===null,'the redo reports one master of cost and is not presented as a review fix');
  ok(JSON.stringify((await parentRef.get()).data())===parentBefore&&done.previousFilms.length===1&&done.previousFilms[0].key==='mobile_square'&&done.previousFilms[0].url.endsWith(first.jobId+'_mobile_square')&&job.previousVersions[0].master.id==='v1_2','the earlier job and its paid square film stay saved and are linked as the previous version');
  await assert.rejects(()=>rd.service.start({...input,redoFormat:'portrait'}),/changed since it was loaded/);n++;
  // Never while another paid film is unfinished; a failed film with no video is offered on its own.
  const current=rd.ref.collection('motionJobs').doc(started.jobId);
  await current.update({masters:{...job.masters,landscape:{...job.masters.landscape,status:'in_progress'}}});
  await assert.rejects(()=>rd.service.start({...W,...rd.scope,redoOf:started.jobId,redoFormat:'portrait',confirmRedo:true}),/still being made and is already paid for/);n++;
  ok((await rd.service.status({...W,...rd.scope})).redoOptions.length===0,'no redo is offered while a paid film is unfinished');
  await current.update({phase:'needs_attention',quality:null,variants:[],error:'landscape animation failed: Provider rejected the scene.',masters:{...job.masters,landscape:{...job.masters.landscape,status:'failed'}}});
  const failed=await rd.service.status({...W,...rd.scope});ok(failed.redoOptions.map(o=>o.format).join()==='landscape'&&failed.redoOptions[0].hasFilm===false,'a size whose film failed offers a redo of that size alone');
  const create1=rd.calls.create,again=await rd.service.start({...W,...rd.scope,redoOf:started.jobId,redoFormat:'landscape',confirmRedo:true});await rd.service.run({...W,jobId:again.jobId});
  const healed=await rd.service.status({...W,...rd.scope}),healedJob=(await rd.ref.collection('motionJobs').doc(again.jobId).get()).data();ok(rd.calls.create===create1+1&&healed.variants.length===3&&healed.phase==='ready'&&healedJob.masters.portrait.id===job.masters.portrait.id&&healedJob.masters.square.id===job.masters.square.id,'redoing the failed size buys one film, keeps the saved masters and completes the set');
 }
 // A re-run, a redo of any one film and a resume ask Google for exactly what a first run asks for: the same URL, method, headers and body (reference photograph, prompt,
 // aspect ratio, output settings) for each orientation. Every new film has its own interaction and its own request id, a resume only polls the saved interaction, a
 // discarded redo never answers a new press, and every start names the job the background worker must be dispatched to.
 {
  const wire=await setup(),W={workspaceId:'design_test'},sent=[],saved=new Set(['v1_pending']);let created=0,busyNow=false;
  const answer=(status,body)=>({ok:status>=200&&status<300,status,headers:{get:()=>null},text:async()=>typeof body==='string'?body:JSON.stringify(body)}),same=(a,b,m)=>{assert.deepStrictEqual(a,b,m);n++;};
  const capacity={error:{code:503,status:'UNAVAILABLE',message:'This model is currently experiencing high demand. Spikes in demand are usually temporary. Please try again later.'}};
  const provider=createGeminiVideo({apiKey:'fixture',sleep:async()=>{},fetch:async(url,o)=>{
   if(url.startsWith('https://storage.googleapis.com/'))return {ok:true,status:200,headers:{get:()=>'video/mp4'},buffer:async()=>Buffer.from('mp4')};
   sent.push({method:o.method,url,headers:o.headers,body:o.body?JSON.parse(o.body):null});
   if(o.method==='POST'){if(busyNow)return answer(503,capacity);const id='v1_wire'+(++created);saved.add(id);return answer(200,{id,status:'in_progress'});}
   const id=url.split('/').pop();return saved.has(id)?answer(200,{id,status:'completed',steps:[{type:'model_output',content:[{type:'video',uri:'https://storage.googleapis.com/'+id+'.mp4'}]}]}):answer(404,{error:{code:404,message:'Interaction not found',status:'NOT_FOUND'}});}});
  wire.D.videoRequest=provider.request;wire.D.videoContent=provider.content;wire.D.sleep=async()=>{};
  const posts=()=>sent.filter(r=>r.method==='POST'),latest=()=>wire.service.status({...W,...wire.scope}),jobRef=id=>wire.ref.collection('motionJobs').doc(id),dispatchable=r=>r.ok===true&&r.queued===true&&r.workspaceId==='design_test'&&/^motion_[a-f0-9]{40}$/.test(r.jobId);
  const first=await wire.service.start({...W,...wire.scope});ok(dispatchable(first),'a first run names the job to dispatch');await wire.service.run({...W,jobId:first.jobId});
  const original=posts().slice(),POST_URL='https://generativelanguage.googleapis.com/v1beta/interactions',byOrientation={portrait:original[0],square:original[1],landscape:original[2]};
  ok(original.length===3&&original.map(r=>r.body.response_format.aspect_ratio).join()==='9:16,16:9,16:9'&&original.every(r=>r.url===POST_URL&&r.headers['x-goog-api-key']==='fixture'&&r.headers['Api-Revision']),'a first run creates one interaction per orientation');
  // Re-run: a new job whose three create requests equal the first run's.
  let state=await latest();const rerun=await wire.service.start({...W,...wire.scope,rerunOf:state.jobId,repairReviewHash:state.repairReviewHash,rerunKey:'wire-rerun-1'});ok(dispatchable(rerun)&&rerun.jobId!==first.jobId,'a re-run is a new job that names itself for dispatch');await wire.service.run({...W,jobId:rerun.jobId});
  same(posts().slice(3),original,'a re-run asks Google for exactly what a first run asks for');
  ok(!/CORRECT THESE EARLIER ISSUES/.test(posts()[3].body.input[1].text),'a fresh re-run carries no earlier-findings instruction, empty or otherwise');
  // Redo of each film: one create, identical to the first run's request for that orientation.
  for(const format of ['portrait','square','landscape']){
   state=await latest();const before=posts().length,redo=await wire.service.start({...W,...wire.scope,redoOf:state.jobId,redoFormat:format,confirmRedo:true});ok(dispatchable(redo),'a redo of the '+format+' film names the job to dispatch');
   ok((await wire.service.run({...W,jobId:redo.jobId})).ok,'the '+format+' redo completes');
   same(posts().slice(before),[byOrientation[format]],'the '+format+' redo asks Google for exactly the first run\'s request for that orientation');
  }
  // Every created film has its own interaction and its own request id; a redo keeps the other films' saved ids and never reuses one.
  const films=new Map();for(const row of (await wire.ref.collection('motionJobs').get()).docs)for(const m of Object.values(row.data().masters||{}))films.set(m.id,m.requestId);
  ok(films.size===created&&created===9&&new Set(films.values()).size===9&&[...films.values()].every(v=>/^[0-9a-f-]{36}$/.test(v)),'nine films, nine interactions, nine unique request ids');
  // Resume (Check saved progress) of a redo whose film is still being made at Google polls that saved interaction, with the same headers, and buys nothing.
  state=await latest();const pend=await wire.service.start({...W,...wire.scope,redoOf:state.jobId,redoFormat:'portrait',confirmRedo:true}),pj=(await jobRef(pend.jobId).get()).data();
  await jobRef(pend.jobId).update({masters:{...pj.masters,portrait:{id:'v1_pending',status:'in_progress',progress:0,size:'720x1280',requestId:'saved',estimatedUsd:1.0136}}});
  const at=sent.length,resumed=await wire.service.start({...W,...wire.scope,resumeJobId:pend.jobId});ok(dispatchable(resumed)&&resumed.jobId===pend.jobId,'a resume names the same job to dispatch');
  ok((await wire.service.run({...W,jobId:pend.jobId})).ok&&sent.length>at&&sent.slice(at).every(r=>r.method==='GET'&&r.url===POST_URL+'/v1_pending')&&created===9,'a resume only polls the saved interaction');
  same(sent[at].headers,original[0].headers,'a poll carries the same headers as a create');ok((await latest()).phase==='ready','the resumed redo completes');
  // Google refusing a create outright for capacity (503 with its own UNAVAILABLE error) never queued the film: the redo stops resumable, and Resume asks again once.
  state=await latest();busyNow=true;const busy=await wire.service.start({...W,...wire.scope,redoOf:state.jobId,redoFormat:'landscape',confirmRedo:true}),refused=await wire.service.run({...W,jobId:busy.jobId});busyNow=false;
  const stopped=await latest();ok(refused.ok===false&&stopped.jobId===busy.jobId&&stopped.phase==='needs_attention'&&stopped.canResume&&!stopped.providerWait&&/did not accept the film request/.test(stopped.error)&&created===9,'a capacity refusal leaves the redo resumable with nothing bought');
  await wire.service.start({...W,...wire.scope,resumeJobId:busy.jobId});ok((await wire.service.run({...W,jobId:busy.jobId})).ok&&created===10&&(await latest()).phase==='ready','Resume asks Google again and buys exactly one film');
  const gateway=createGeminiVideo({apiKey:'fixture',sleep:async()=>{},fetch:async()=>answer(503,'<html>Bad gateway</html>')});
  await assert.rejects(()=>gateway.request('interactions','POST',{}),e=>e.transient===true&&e.definiteResponse===false&&/nothing is requested again/.test(e.message));n++;
  // A redo that was discarded (for example while its create was unconfirmed) never answers the next press: that press is a new attempt that buys one film.
  state=await latest();const stuck=await wire.service.start({...W,...wire.scope,redoOf:state.jobId,redoFormat:'square',confirmRedo:true});
  ok((await wire.service.start({...W,...wire.scope,redoOf:state.jobId,redoFormat:'square',confirmRedo:true})).jobId===stuck.jobId,'the same press sent twice shares its one job');
  await jobRef(stuck.jobId).update({phase:'needs_attention',inFlight:{orientation:'square',requestId:'unconfirmed',at:1},error:'Google did not confirm the film request.'});
  await wire.service.start({...W,...wire.scope,discardJobId:stuck.jobId,confirmDiscard:true});
  state=await latest();const c0=created,again=await wire.service.start({...W,...wire.scope,redoOf:state.jobId,redoFormat:'square',confirmRedo:true});
  ok(dispatchable(again)&&again.jobId!==stuck.jobId&&!(await jobRef(again.jobId).get()).data().resetAt,'a redo after a discarded one is a new job, not the cancelled one');
  ok((await wire.service.run({...W,jobId:again.jobId})).ok&&created===c0+1&&(await latest()).jobId===again.jobId&&(await latest()).phase==='ready','the new attempt buys one film and becomes the current animation');
  same(posts().slice(-1),[byOrientation.square],'and its request is the first run\'s square request');
 }
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
  ok(/60-75% of frame width/.test(prompt)&&/55-70% of the height of a centred square crop/.test(prompt)&&/THE PIECE IS THE HERO AND FACES THE CAMERA/.test(prompt),'the film is told to frame the piece large as the hero, so it is never shot distant');
  ok(/front face square to the lens, completely visible/.test(prompt)&&/never lies down or flat on a surface, never turns edge-on/.test(prompt)&&(prompt.match(/^RULE \d+ - /gm)||[]).length===5,'the film is told the piece stands upright facing the camera and never lies flat or edge-on');
  ok(/60-70% of frame height/.test(motion.motionPrompt(job,'landscape',''))&&/55-70% of its height/.test(motion.motionPrompt({...job,creativeDirection:Object.fromEntries(keys.map(k=>[k,'clean '+k]))},'square','')),'landscape and square films also frame the piece large');
  {const lying=motion.scrubDirection({title:'Crocodile Charm Pendant',plan:{},creativeDirection:{...Object.fromEntries(keys.map(k=>[k,'clean '+k])),opening:'The piece lying flat on warm sand, waves behind.',middle:'Sunlight sweeps the sand.'}},'portrait');
   ok(!/lying|flat/.test(lying.opening||'')&&lying.middle==='Sunlight sweeps the sand.','a treatment clause that lays the piece down is removed before the film model sees it');
   ok(/Never lying down or flat on a surface/.test(motion.motionRequest({pipelineVersion:3}).input[0].content)&&/stands upright with its front face square to the camera/.test(motion.motionRequest({pipelineVersion:3}).input[0].content),'the treatment planner is told to stage the piece upright and facing the camera');}
  {const life=motion.scrubDirection({title:'Crocodile Charm Pendant',plan:{},creativeDirection:{...Object.fromEntries(keys.map(k=>[k,'clean '+k])),props:'Green leaves swaying, a plain towel stirring in the breeze, the chain swaying gently, water rippling.'}},'portrait'),req=motion.motionRequest({pipelineVersion:3}).input[0].content;
   ok(/GENTLE LIFE IN THE SCENE\.\nWhile the piece stays steady, one to three supporting things/.test(prompt)&&/never excessive/.test(prompt)&&/Nothing covers or touches the front face/.test(prompt)&&/GENTLE LIFE IN THE SCENE, WHICHEVER RULE/.test(req)&&/one to three supporting props/.test(req)&&/never steals focus/.test(req),'planner and film are told to keep one to three supporting props moving gently, not excessive, clear of the charm face');
   ok(life.props==='Green leaves swaying, a plain towel stirring in the breeze, the chain swaying gently, water rippling.','the scrub keeps supporting prop movement in the treatment');
   ok(/chain or cord that the piece itself hangs from is part of that same one piece/.test(prompt)&&/A charm sold alone must never imply an included chain/.test(req)&&/never fail or deduct for it/.test(require('fs').readFileSync(require('path').join(__dirname,'../../netlify/functions/googleAdsAdMotion.js'),'utf8'))&&/never a reason to fail/.test(require('../../netlify/functions/googleAdsSinglePiece').REVIEW_RULE),'a chain the piece hangs from is the same single piece, moving props are never a second piece or a review failure');}
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
  // Every master is filmed from the SAME identity reference (the saved design's first own-listing photograph, full quality, upright), and a format whose
  // charm has drifted from it (a plain silhouette with no engraving, eye or wing line) fails the review by name, so only that film is regenerated.
  {
   const identity=require('../../netlify/functions/googleAdsAdIdentity'),colour=async data=>[...await sharp(Buffer.from(data,'base64')).resize(1,1).raw().toBuffer()],photo=(width,height,background)=>sharp({create:{width,height,channels:3,background}}).jpeg().toBuffer();
   const photos={first:await photo(3000,2000,'#c9a54a'),second:await photo(400,600,'#203040'),third:await photo(500,500,'#2a6a3a')},reference=async c=>(await c.ref.collection('editorAIJobs').doc(c.id).collection('data').doc('request').set({sources:[{asset:{path:'photo'}}],identitySources:[{id:'a',asset:{path:'first'},width:3000,height:2000},{id:'b',asset:{path:'second'},width:400,height:600},{id:'c',asset:{path:'third'},width:500,height:500}]}),await c.ref.collection('editorAIJobs').doc(c.id).collection('data').doc('result').set({responsive:{plan:{nativeCopy:{headlines:['Pendant','A personal touch']},layouts:[]}},sources:[{id:'b',asset:{path:'second'},width:400,height:600},{id:'c',asset:{path:'third'},width:500,height:500},{id:'a',asset:{path:'first'},width:3000,height:2000}]}),c.D.loadAsset=async a=>photos[a.path]||photos.first);
   const imageOf=body=>body.input.find(i=>i.type==='image').data;
   let drifted=true;const dr=await setup();await reference(dr);
   dr.D.reviewImages=async(source,files,brief)=>{dr.calls.review++;dr.lastBrief=brief;const formats=[...new Set(brief.renderedFormats.map(f=>f.key))],none={summary:'',deductions:[]};
    return {pass:true,productFaithful:true,mobileReadable:true,score:100,scores:{messaging:100,layout:100,relevance:100,visualAppeal:100,productRecognition:100},categoryReviews:{messaging:none,layout:none,relevance:none,visualAppeal:none,productRecognition:none},issues:[],
     formatIdentity:Object.fromEntries(formats.map(k=>{const bad=drifted&&k==='mobile_square';return [k,{sameOutline:!bad,sameEngraving:!bad,sameFeatures:true,evidence:bad?'The charm is a plain silhouette with no wing line or eye, unlike the reference.':'Matches the reference.'}];}))};};
   const st=await dr.service.start({workspaceId:'design_test',...dr.scope});await dr.service.run({workspaceId:'design_test',jobId:st.jobId});
   const images=dr.calls.bodies.map(imageOf),first=await colour(images[0]);
   ok(dr.calls.bodies.length===3&&images.every(i=>i===images[0]),'portrait, square and landscape are filmed from byte-identical reference pixels');
   ok(first[0]>first[2]+40&&(await sharp(Buffer.from(images[0],'base64')).metadata()).width===2048,'that reference is the first saved photograph of the listing, kept at full quality rather than shrunk to 1280');
   ok(dr.calls.bodies.every(b=>/every size must match the attached photograph exactly/.test(b.input.find(i=>i.type==='text').text)&&/every engraved line and marking it shows/.test(b.input.find(i=>i.type==='text').text)),'every format carries the same identity rules');
   ok(/one format at a time/.test(identity.formatRule(['mobile_square']))&&Object.keys(identity.formatIdentitySchema(['mobile_portrait','mobile_square','desktop_landscape']).properties).join()==='mobile_portrait,mobile_square,desktop_landscape'&&identity.formatIdentitySchema(['mobile_square']).properties.mobile_square.required.join()==='sameOutline,sameEngraving,sameFeatures,evidence','the review asks for one outline, engraving and features verdict per format');
   const state=await dr.service.status({workspaceId:'design_test',...dr.scope}),pr=state.quality.categoryReviews.productRecognition;
   ok(state.quality.pass===false&&state.quality.productFaithful===false&&!state.qualityTargetMet&&state.quality.formatIdentityFailures.join()==='mobile_square'&&state.quality.issues.some(i=>/square film/.test(i)&&/mobile_square/.test(i)),'a square whose charm lacks the reference engraving fails a perfect-scoring set and is named');
   ok(pr.deductions.length===1&&pr.deductions[0].formats.join()==='mobile_square'&&pr.deductions.reduce((n,d)=>n+d.points,0)===100-state.quality.scores.productRecognition&&state.quality.score===99,'the failed format is an explained product deduction, not a hidden gate');
   const option=state.fixOptions[0];ok(state.fixOptions.length===1&&option.kind==='master'&&option.orientation==='square'&&option.formats.join()==='mobile_square'&&/only the square film/.test(option.label),'the existing targeted fix offers to regenerate only the square film');
   drifted=false;const before=dr.calls.create,fixed=await dr.service.start({workspaceId:'design_test',...dr.scope,fixOf:st.jobId,repairReviewHash:state.repairReviewHash,fix:{category:'productRecognition',index:0}}),parent=(await dr.ref.collection('motionJobs').doc(st.jobId).get()).data();
   ok((await dr.service.run({workspaceId:'design_test',jobId:fixed.jobId})).ok&&dr.calls.create===before+1&&dr.calls.aspects.slice(-1)[0]==='16:9','the fix buys one 16:9 master only');
   const child=(await dr.ref.collection('motionJobs').doc(fixed.jobId).get()).data(),repaired=dr.calls.bodies.slice(-1)[0];
   ok(JSON.stringify(child.originalSources)===JSON.stringify(parent.originalSources)&&imageOf(repaired)===images[0],'the fix keeps the same identity reference for the regenerated film');
   ok(/CORRECT THESE EARLIER ISSUES/.test(repaired.input.find(i=>i.type==='text').text)&&/same outline and proportions/.test(repaired.input.find(i=>i.type==='text').text)&&dr.lastBrief.targetedFix.formats.join()==='mobile_square','the regenerated film is told what was missing and the review is told which format was corrected');
   const after=await dr.service.status({workspaceId:'design_test',...dr.scope});ok(after.quality.pass===true&&after.qualityTargetMet&&!after.fixOptions.length&&child.masters.portrait.id===parent.masters.portrait.id&&child.masters.landscape.id===parent.masters.landscape.id,'once every format matches, the set passes and the other films are the saved originals');
   // Adapter: the motion review carries the per-format verdict schema and rule, and gates on it.
   const {createAdDesignAdapters,TEXT_MODEL}=require('../../netlify/functions/googleAdsAdDesignAdapters'),{Readable}=require('node:stream'),sent=[],stream=text=>Readable.from([{type:'message_start',message:{id:'m',type:'message',role:'assistant',model:TEXT_MODEL,content:[],usage:{input_tokens:10,output_tokens:1}}},{type:'content_block_start',index:0,content_block:{type:'text',text:''}},{type:'content_block_delta',index:0,delta:{type:'text_delta',text}},{type:'content_block_stop',index:0},{type:'message_delta',delta:{stop_reason:'end_turn'},usage:{output_tokens:10}},{type:'message_stop'}].map(e=>'event: '+e.type+'\ndata: '+JSON.stringify(e)+'\n\n'));
   const rawReview={scores:{messaging:100,layout:100,relevance:100,visualAppeal:100,productRecognition:100},categoryReviews:Object.fromEntries(['messaging','layout','relevance','visualAppeal','productRecognition'].map(k=>[k,{summary:'Fine.',deductions:[]}])),productRecognizable:true,claimsSupported:true,mobileReadable:true,exactProductIdentity:true,footageLettering:false,multipleProducts:false,issues:[],formatIdentity:{mobile_portrait:{sameOutline:true,sameEngraving:true,sameFeatures:true,evidence:'Matches.'},mobile_square:{sameOutline:false,sameEngraving:false,sameFeatures:true,evidence:'Plain silhouette.'}}};
   const adapters=createAdDesignAdapters({env:{OPENAI_API_KEY:'fixture-only',ANTHROPIC_API_KEY:'fixture-only'},sleep:async()=>{},log:()=>{},formats:[],fetch:async(url,options)=>{sent.push(JSON.parse(options.body));return {ok:true,status:200,headers:{get:()=>null},body:stream(JSON.stringify(rawReview))};}});
   const reviewed=await adapters.reviewImages(photos.second,[photos.second],{reviewType:'complete_ad',motionReview:'x',renderedFormats:[{key:'mobile_portrait'},{key:'mobile_square'}]},[photos.second],'per-format-identity');
   const wire=JSON.stringify(sent[0]);ok(/PER-FORMAT CHARM IDENTITY/.test(wire)&&sent[0].output_config.format.schema.required.includes('formatIdentity')&&Object.keys(sent[0].output_config.format.schema.properties.formatIdentity.properties).join()==='mobile_portrait,mobile_square','the animated review request asks for a verdict for every rendered format');
   ok(reviewed.pass===false&&reviewed.productFaithful===false&&reviewed.formatIdentityFailures.join()==='mobile_square'&&reviewed.categoryReviews.productRecognition.deductions[0].formats.join()==='mobile_square','the review result fails only the named format even though the set-level identity flag said yes');
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

