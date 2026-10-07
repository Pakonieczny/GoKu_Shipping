'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const qa=require('../../concierge-avatar-qa.js');
const avatarFactory=require('../../brites-concierge-avatar.js');
const {JSDOM}=require('jsdom');

const webglSnapshot=(overrides={})=>({mode:'webgl',state:'idle',visible:true,intersecting:true,reducedMotion:false,animated:true,frames:120,frameRenderMs:5.2,pixelRatio:2,quality:{name:'high',pixelRatio:2,shadowSize:2048,textureSize:2048,fps:60},textures:[{name:'Porcelain',width:2048,height:2048},{name:'Iris',width:2048,height:2048}],shadow:{enabled:true,size:2048,type:'PCF soft',casts:true,receivingStage:true},contextLost:false,...overrides});
const gl={available:true,contextType:'webgl2',vendor:'Fixture GPU vendor',renderer:'Fixture renderer',version:'WebGL 2.0 fixture',shadingLanguageVersion:'WebGL GLSL ES 3.00',maxTextureSize:16384,maxRenderbufferSize:16384,depthBits:24,maxCombinedTextureUnits:32,depthTexture:true,contextLost:false};

test('WebGL report exposes observed identity, shadow capability, textures, ratio and sampled frames without claiming visual quality',()=>{
  const report=qa.buildDiagnostics({before:webglSnapshot(),after:webglSnapshot({frames:165}),gl,display:{devicePixelRatio:2,cssWidth:400,bufferWidth:800},sampleMs:750,performance:{fps:[58,60,59],renderMs:[5,6,8],firstFrame:{status:'ready_event_observed',milliseconds:412,relativeTo:'avatar_create'},longTasks:{supported:true,count:1,totalMs:55,maxMs:55}}});
  assert.equal(report.webgl.vendor,'Fixture GPU vendor');assert.equal(report.webgl.renderer,'Fixture renderer');assert.equal(report.webgl.status,'available');
  assert.equal(report.shadowMap.capabilityObserved,true);assert.equal(report.shadowMap.status,'capability_observed_visual_unverified');assert.equal(report.shadowMap.appearance,'unverified');
  assert.equal(report.textures.largestDeclaredSize,2048);assert.equal(report.textures.fitsObservedCapability,true);assert.equal(report.pixelRatio.backingStore,2);
  assert.equal(report.frameSampling.frameDelta,45);assert.equal(report.frameSampling.effectiveFps,60);assert.equal(report.frameSampling.status,'sampled');
  assert.equal(report.frameSampling.sampleCount,3);assert.equal(report.frameSampling.medianFps,59);assert.equal(report.frameSampling.p95FrameRenderMs,7.8);assert.equal(report.frameSampling.firstFrame.milliseconds,412);
  assert.deepEqual(report.longTasks,{supported:true,count:1,totalMs:55,maxMs:55,thresholdMs:50});
  assert.equal(report.gpu.visualQuality,'unverified');assert.equal(report.acceptance.gpu,'capability_observed_visual_unverified');
});

test('disabled WebGL preserves an explicit animated 2-D fallback and leaves GPU, shadows and FPS unverified',()=>{
  const declarations={schema:1,source:'loaded_scene_module',textures:['Porcelain','Gold grain','Gold roughness','Iris'].map(name=>({name,kind:'fixture',width:2048,height:2048}))};
  const report=qa.buildDiagnostics({before:{mode:'fallback'},after:{mode:'fallback',visible:true,intersecting:true,reducedMotion:false,quality:{textureSize:2048,pixelRatio:2,fps:60},declarations},gl:{available:false},frameRendering:'fallback',frameHidden:false,sampleMs:750});
  assert.equal(report.webgl.status,'webgl_unavailable');assert.equal(report.webgl.vendor,null);assert.equal(report.webgl.renderer,null);
  assert.equal(report.staticFallback.active,true);assert.equal(report.staticFallback.visible,true);assert.equal(report.staticFallback.preserved,true);
  assert.equal(report.acceptance.gpu,'unverified');assert.equal(report.acceptance.shadows,'unverified');assert.equal(report.acceptance.fps,'unverified');
  assert.equal(report.textures.declaredMaps,4);assert.equal(report.textures.observedCreatedMaps,0);assert.equal(report.textures.declarationSource,'loaded_scene_module');assert.equal(report.textures.largestDeclaredSize,2048);assert.equal(report.textures.fitsObservedCapability,null);
  assert.equal(report.shadowMap.status,'unverified_webgl_unavailable');assert.equal(report.frameSampling.status,'unverified_webgl_unavailable');assert.match(report.acceptance.message,/Animated 2-D fallback is active.*unverified/i);
});

test('reduced motion and hidden rendering are distinguished from failed frame sampling',()=>{
  const reduced=qa.buildDiagnostics({before:webglSnapshot(),after:webglSnapshot({frames:120,reducedMotion:true,animated:false}),gl,sampleMs:750});assert.equal(reduced.frameSampling.status,'intentionally_paused_reduced_motion');assert.equal(reduced.motion.reducedMotion,true);assert.equal(reduced.acceptance.fps,'unverified');
  const hidden=qa.buildDiagnostics({before:webglSnapshot(),after:webglSnapshot({frames:120,visible:false,animated:false}),gl,sampleMs:750});assert.equal(hidden.frameSampling.status,'paused_not_visible');assert.equal(hidden.motion.visible,false);
});

test('observed context loss fails closed to fallback and cannot be reported as a GPU, shadow or FPS pass',()=>{
  const report=qa.buildDiagnostics({before:webglSnapshot(),after:webglSnapshot({mode:'fallback',frames:121,animated:false,contextLost:true}),gl:{...gl,contextLost:true},frameRendering:'fallback',frameHidden:false,sampleMs:750});
  assert.equal(report.contextLoss.status,'lost_fallback_required');assert.equal(report.contextLoss.forcedForTest,false);assert.equal(report.gpu.status,'unverified_context_lost');assert.equal(report.shadowMap.status,'unverified_context_lost');assert.equal(report.frameSampling.status,'unverified_context_lost');assert.equal(report.staticFallback.preserved,true);
});

test('missing canvas is reported unavailable without creating or probing a replacement context',()=>{assert.deepEqual(qa.inspectWebGL({querySelector:()=>null}),{available:false});assert.deepEqual(qa.inspectWebGL(null),{available:false});});

test('hidden on-demand avatar diagnostics return immediately as deferred rather than hanging or claiming WebGL failure',async()=>{
  const avatar={ready:new Promise(()=>{}),element:{hidden:true,dataset:{rendering:'pending'},querySelector:()=>null},snapshot:()=>({mode:'pending',visible:false,intersecting:true,loading:false,destroyed:false,frames:0})};
  const started=Date.now(),report=await qa.collect({avatar,sampleMs:1,readyWaitMs:1000});
  assert.ok(Date.now()-started<250,'hidden on-demand diagnostics must not wait for the unresolved ready promise');
  assert.deepEqual(report.runtime,{readiness:'deferred_not_visible',waitMs:0,loading:false,destroyed:false,onDemandDeferred:true});
  assert.equal(report.webgl.status,'renderer_pending');assert.equal(report.gpu.status,'unverified_renderer_pending');assert.equal(report.staticFallback.active,false);assert.equal(report.acceptance.gpu,'unverified');
});

test('visible pending avatar diagnostics use a bounded readiness wait and preserve pending truthfulness',async()=>{
  const avatar={ready:new Promise(()=>{}),element:{hidden:false,dataset:{rendering:'loading'},querySelector:()=>null},snapshot:()=>({mode:'pending',visible:true,intersecting:true,loading:true,destroyed:false,frames:0})};
  const report=await qa.collect({avatar,sampleMs:1,readyWaitMs:5});
  assert.equal(report.runtime.readiness,'timed_out');assert.equal(report.runtime.loading,true);assert.equal(report.runtime.onDemandDeferred,false);
  assert.equal(report.webgl.status,'renderer_pending');assert.equal(report.frameSampling.status,'unverified_renderer_pending');assert.equal(report.acceptance.message,'Renderer capability is pending; GPU rendering, shadows and FPS are not yet verified.');
});

test('visual QA pages expose the diagnostics and explicitly prohibit fallback GPU claims',()=>{
  const root=path.join(__dirname,'../..'),studio=fs.readFileSync(path.join(root,'concierge-avatar-qa.html'),'utf8'),checklist=fs.readFileSync(path.join(root,'concierge-avatar-checklist.html'),'utf8');
  assert.match(studio,/concierge-avatar-qa\.js/);assert.match(studio,/Animated 2-D fallback confirmed\. GPU rendering, shadows and FPS were not tested\./);assert.match(studio,/BritesAvatarAcceptance\.collect/);
  assert.match(studio,/id="pause"[^>]*>Pause animation/);assert.match(studio,/id="hide"[^>]*>Hide guide/);assert.match(studio,/id="run-states"[^>]*>Run all states/);assert.match(studio,/sampleMs:15000/);assert.match(studio,/visualCertification:'not_performed'/);
  for(const term of ['vendor','renderer','shadowMap','textures','pixelRatio','frameSampling','reduced-motion','contextLoss','unverified'])assert.match(checklist,new RegExp(term,'i'),term);
  assert.match(checklist,/never changes browser graphics settings or fingerprinting/i);assert.match(checklist,/never forces a context loss/i);
});

test('bounded export package keeps only acceptance evidence and never upgrades visual claims',()=>{
  const diagnostics={...qa.buildDiagnostics({before:webglSnapshot(),after:webglSnapshot({frames:180}),gl,display:{devicePixelRatio:2,cssWidth:400,bufferWidth:800},sampleMs:1000}),collectedAt:'2026-10-02T12:00:00.000Z',secret:'must-not-export',webgl:{...qa.buildDiagnostics({before:webglSnapshot(),after:webglSnapshot({frames:180}),gl,sampleMs:1000}).webgl,renderer:'Fixture renderer\u0000'.padEnd(500,'X'),privateUrl:'https://private.invalid/token'}};
  const stateEvidence={status:'state_pipeline_and_frames_observed_visual_unverified',states:['idle','listening','thinking','speaking','success','error'].map(state=>({state,observedState:state,mode:'webgl',frameDelta:4,renderObserved:true,raw:'omit'})),gestures:[{name:'speaking_arm_motion',state:'speaking',renderObserved:true,unsafe:'omit'},{name:'success_arm_lift',state:'success',renderObserved:true}]};
  const exported=qa.serializeAcceptancePackage({diagnostics,stateEvidence});
  assert.equal(exported.value.evidenceLevel,'webgl_runtime_observed_visual_review_required');assert.equal(exported.value.webgl.vendor,'Fixture GPU vendor');assert.equal(exported.value.webgl.renderer.length,160);
  assert.equal(exported.value.shadows.capabilityObserved,true);assert.equal(exported.value.textures.fitsObservedCapability,true);assert.equal(exported.value.frameSample.status,'sampled');
  assert.equal(exported.value.frameSample.firstFrame.status,'not_observed');assert.equal(exported.value.longTasks.supported,false);
  assert.equal(exported.value.runtime.readiness,'not_recorded');assert.equal(exported.value.runtime.onDemandDeferred,false);
  assert.equal(exported.value.expressionsAndGestures.states.length,6);assert.equal(exported.value.expressionsAndGestures.gestures.length,2);assert.equal(exported.value.claims.gestureAppearance,'unverified');
  assert.ok(exported.bytes<24576);assert.doesNotMatch(exported.json,/must-not-export|private\.invalid|privateUrl|unsafe|raw/);assert.doesNotMatch(exported.json,/\u0000/);
});

test('fallback export remains truthful and omits unavailable GPU identity',()=>{
  const diagnostics={...qa.buildDiagnostics({before:{mode:'fallback'},after:{mode:'fallback',visible:true,intersecting:true,quality:{textureSize:2048},declarations:{textures:[{name:'Declared',width:2048,height:2048}]}},gl:{available:false},frameRendering:'fallback'}),collectedAt:'2026-10-02T12:00:00.000Z'};
  const value=qa.acceptancePackage({diagnostics,stateEvidence:{status:'state_pipeline_observed_render_unverified',states:[{state:'speaking',observedState:'speaking',mode:'fallback',frameDelta:99,renderObserved:false}]}});
  assert.equal(value.evidenceLevel,'fallback_only_webgl_unverified');assert.equal(value.webgl.vendor,null);assert.equal(value.webgl.renderer,null);assert.equal(value.shadows.capabilityObserved,false);assert.equal(value.frameSample.status,'unverified_webgl_unavailable');assert.equal(value.pauseAndFallback.fallbackActive,true);assert.equal(value.expressionsAndGestures.states[0].renderObserved,false);assert.equal(value.claims.gpuAppearance,'unverified');
});

test('state exercise restores the original expression and records rendered gesture states',async()=>{
  let current='thinking',frames=10;const avatar={snapshot:()=>({state:current,mode:'webgl',frames,contextLost:false}),setState:value=>{current=value;frames++;}};
  const evidence=await qa.exerciseStates({avatar,win:{setTimeout:fn=>{frames+=2;fn();}},settleMs:32});
  assert.equal(current,'thinking');assert.equal(evidence.states.length,6);assert.ok(evidence.states.every(row=>row.observedState===row.state&&row.renderObserved));assert.equal(evidence.status,'state_pipeline_and_frames_observed_visual_unverified');assert.deepEqual(evidence.gestures.map(item=>item.name),['speaking_arm_motion','success_arm_lift']);
});

test('15-second sampler emits bounded aggregate performance, first-frame and long-task evidence',async()=>{
  let frames=0,clock=0,observerDisconnected=false;
  class PerformanceObserver{constructor(callback){this.callback=callback;}observe(){this.callback({getEntries:()=>[{duration:61.25},{duration:12}]});}disconnect(){observerDisconnected=true;}}
  const win={PerformanceObserver,performance:{now:()=>clock},setTimeout(callback,delay){frames++;clock+=delay;callback();},clearTimeout(){},matchMedia:()=>({matches:false}),devicePixelRatio:2};
  const avatar={ready:Promise.resolve(),element:{hidden:false,dataset:{rendering:'webgl'},querySelector:()=>null},snapshot:()=>({mode:'webgl',state:'idle',visible:true,intersecting:true,paused:false,reducedMotion:false,animated:true,frames,frameRenderMs:4+frames/10,quality:{fps:60}})};
  const report=await qa.collect({avatar,win,sampleMs:15000,sampleIntervalMs:250,knownFirstFrameMs:475});
  assert.equal(report.frameSampling.sampleMs,15000);assert.equal(report.frameSampling.measuredMs,15000);assert.equal(report.frameSampling.sampleCount,60);assert.equal(report.frameSampling.medianFps,4);assert.equal(report.frameSampling.firstFrame.status,'ready_event_observed');assert.equal(report.frameSampling.firstFrame.milliseconds,475);
  assert.equal(report.longTasks.supported,true);assert.equal(report.longTasks.count,1);assert.equal(report.longTasks.totalMs,61.25);assert.equal(observerDisconnected,true);
  const packaged=qa.acceptancePackage({diagnostics:report,stateEvidence:{states:[]}});assert.equal(packaged.frameSample.sampleCount,60);assert.equal(packaged.frameSample.measuredMs,15000);assert.equal(packaged.longTasks.maxMs,61.25);assert.equal(packaged.claims.gpuAppearance,'unverified');
});

test('state sweep fails safely while the visible guide is paused or hidden',async()=>{
  for(const snapshot of [{state:'idle',visible:false,paused:false},{state:'idle',visible:true,paused:true}]){
    let changed=false;const result=await qa.exerciseStates({avatar:{snapshot:()=>snapshot,setState:()=>{changed=true;}}});
    assert.equal(result.status,'blocked_avatar_not_active');assert.equal(result.visualAppearance,'unverified');assert.equal(changed,false);
  }
});

test('copy and download export only the regenerated sanitized bounded package',async()=>{
  const diagnostics={...qa.buildDiagnostics({before:webglSnapshot(),after:webglSnapshot({frames:150}),gl,sampleMs:500}),collectedAt:'2026-10-02T12:00:00.000Z'},input={diagnostics,stateEvidence:{states:[]}},writes=[];
  const copied=await qa.copyAcceptancePackage(input,{navigator:{clipboard:{writeText:async value=>writes.push(value)}}});assert.equal(copied.copied,true);assert.equal(writes.length,1);assert.match(writes[0],/brites_avatar_runtime_acceptance/);
  let clicked=false,revoked=null,blob=null;const downloaded=qa.downloadAcceptancePackage(input,{document:{createElement:()=>({click(){clicked=true;}})},URL:{createObjectURL:value=>(blob=value,'blob:fixture'),revokeObjectURL:value=>{revoked=value;}},Blob:class{constructor(parts,options){this.parts=parts;this.options=options;}}});
  assert.equal(downloaded.filename,'brites-avatar-acceptance.json');assert.equal(clicked,true);assert.equal(revoked,'blob:fixture');assert.equal(blob.options.type,'application/json');assert.ok(downloaded.bytes<24576);
});

test('CPU pose audit confirms distinct bounded state contracts without certifying their appearance',()=>{
  const report=qa.auditCpuPoses({poseFor:avatarFactory.poseFor,states:avatarFactory.STATES});
  assert.equal(report.status,'cpu_pose_contract_observed_visual_unverified');
  assert.equal(report.evidenceLevel,'cpu_only_visual_unverified');
  assert.equal(report.states.length,6);
  assert.ok(Object.values(report.checks).every(Boolean));
  assert.equal(report.claims.expressionAppearance,'unverified');
  assert.equal(report.claims.gestureAppearance,'unverified');
});

test('CPU audit rejects opening mouths or invented silent speech energy',()=>{
  for(const failure of ['opening','silent-energy']){
    const report=qa.auditCpuPoses({states:avatarFactory.STATES,poseFor:input=>{
      const pose=avatarFactory.poseFor(input);
      if(input.state==='speaking'&&failure==='opening')return{...pose,mouth:'open',mouthOpen:.4};
      if(input.state==='speaking'&&input.level===0&&failure==='silent-energy')return{...pose,speechEnergy:.8};
      return pose;
    }});
    assert.equal(report.status,'cpu_pose_contract_not_confirmed');
    assert.equal(report.claims.expressionAppearance,'unverified');
    assert.equal(report.claims.gestureAppearance,'unverified');
  }
});

test('source declaration audit checks PBR, shadows, deformation and safe fallback controls without a GPU claim',()=>{
  const root=path.join(__dirname,'../..'),controllerSource=fs.readFileSync(path.join(root,'brites-concierge-avatar.js'),'utf8');
  for(const relative of ['brites-concierge-avatar-scene.mjs','assets/brites-concierge-avatar-scene.mjs']){
    const report=qa.auditSceneSource({sceneSource:fs.readFileSync(path.join(root,relative),'utf8'),controllerSource});
    assert.equal(report.status,'source_contract_declared_visual_unverified',relative);
    assert.equal(report.evidenceLevel,'source_only_visual_unverified');
    assert.ok(Object.values(report.checks).every(Boolean));
    assert.equal(report.claims.gpuAppearance,'unverified');
    assert.equal(report.claims.shadowAppearance,'unverified');
    assert.equal(report.claims.materialAppearance,'unverified');
  }
});

test('DOM control audit exercises every expression plus pause and live fallback semantics, then restores state',async t=>{
  const dom=new JSDOM('<!doctype html><main id="avatar"></main>',{url:'https://growth-sandbox.example/concierge-avatar-qa.html',pretendToBeVisual:true});
  const {window}=dom;window.matchMedia=()=>({matches:false,addEventListener(){},removeEventListener(){}});window.IntersectionObserver=class{observe(){}disconnect(){}};
  const avatar=avatarFactory.create({container:window.document.querySelector('#avatar'),visible:true,greetingOnOpen:false,loadScene:async()=>{throw Error('Synthetic WebGL unavailable');}});
  t.after(()=>{avatar.destroy();window.close();});
  await avatar.ready;avatar.setState('thinking');
  const report=await qa.exerciseDomControls({avatar,win:window,settleMs:16});
  assert.equal(report.status,'dom_state_pause_fallback_contract_observed_visual_unverified');
  assert.equal(report.evidenceLevel,'dom_only_visual_unverified');
  assert.equal(report.mode,'fallback');
  assert.ok(Object.values(report.checks).every(Boolean));
  assert.equal(report.states.length,6);
  assert.equal(avatar.snapshot().state,'thinking');
  assert.equal(avatar.snapshot().paused,false);
  assert.equal(report.claims.fallbackAppearance,'unverified');
});
