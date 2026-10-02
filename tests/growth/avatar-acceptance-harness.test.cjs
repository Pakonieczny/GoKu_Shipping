'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const qa=require('../../concierge-avatar-qa.js');

const webglSnapshot=(overrides={})=>({mode:'webgl',state:'idle',visible:true,intersecting:true,reducedMotion:false,animated:true,frames:120,frameRenderMs:5.2,pixelRatio:2,quality:{name:'high',pixelRatio:2,shadowSize:2048,textureSize:2048,fps:60},textures:[{name:'Porcelain',width:2048,height:2048},{name:'Iris',width:2048,height:2048}],shadow:{enabled:true,size:2048,type:'PCF soft',casts:true,receivingStage:true},contextLost:false,...overrides});
const gl={available:true,contextType:'webgl2',vendor:'Fixture GPU vendor',renderer:'Fixture renderer',version:'WebGL 2.0 fixture',shadingLanguageVersion:'WebGL GLSL ES 3.00',maxTextureSize:16384,maxRenderbufferSize:16384,depthBits:24,maxCombinedTextureUnits:32,depthTexture:true,contextLost:false};

test('WebGL report exposes observed identity, shadow capability, textures, ratio and sampled frames without claiming visual quality',()=>{
  const report=qa.buildDiagnostics({before:webglSnapshot(),after:webglSnapshot({frames:165}),gl,display:{devicePixelRatio:2,cssWidth:400,bufferWidth:800},sampleMs:750});
  assert.equal(report.webgl.vendor,'Fixture GPU vendor');assert.equal(report.webgl.renderer,'Fixture renderer');assert.equal(report.webgl.status,'available');
  assert.equal(report.shadowMap.capabilityObserved,true);assert.equal(report.shadowMap.status,'capability_observed_visual_unverified');assert.equal(report.shadowMap.appearance,'unverified');
  assert.equal(report.textures.largestDeclaredSize,2048);assert.equal(report.textures.fitsObservedCapability,true);assert.equal(report.pixelRatio.backingStore,2);
  assert.equal(report.frameSampling.frameDelta,45);assert.equal(report.frameSampling.effectiveFps,60);assert.equal(report.frameSampling.status,'sampled');
  assert.equal(report.gpu.visualQuality,'unverified');assert.equal(report.acceptance.gpu,'capability_observed_visual_unverified');
});

test('disabled WebGL preserves an explicit static fallback and leaves GPU, shadows and FPS unverified',()=>{
  const declarations={schema:1,source:'loaded_scene_module',textures:['Porcelain','Gold grain','Gold roughness','Iris'].map(name=>({name,kind:'fixture',width:2048,height:2048}))};
  const report=qa.buildDiagnostics({before:{mode:'fallback'},after:{mode:'fallback',visible:true,intersecting:true,reducedMotion:false,quality:{textureSize:2048,pixelRatio:2,fps:60},declarations},gl:{available:false},frameRendering:'fallback',frameHidden:false,sampleMs:750});
  assert.equal(report.webgl.status,'webgl_unavailable');assert.equal(report.webgl.vendor,null);assert.equal(report.webgl.renderer,null);
  assert.equal(report.staticFallback.active,true);assert.equal(report.staticFallback.visible,true);assert.equal(report.staticFallback.preserved,true);
  assert.equal(report.acceptance.gpu,'unverified');assert.equal(report.acceptance.shadows,'unverified');assert.equal(report.acceptance.fps,'unverified');
  assert.equal(report.textures.declaredMaps,4);assert.equal(report.textures.observedCreatedMaps,0);assert.equal(report.textures.declarationSource,'loaded_scene_module');assert.equal(report.textures.largestDeclaredSize,2048);assert.equal(report.textures.fitsObservedCapability,null);
  assert.equal(report.shadowMap.status,'unverified_webgl_unavailable');assert.equal(report.frameSampling.status,'unverified_webgl_unavailable');assert.match(report.acceptance.message,/Static fallback is active.*unverified/i);
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

test('visual QA pages expose the diagnostics and explicitly prohibit fallback GPU claims',()=>{
  const root=path.join(__dirname,'../..'),studio=fs.readFileSync(path.join(root,'concierge-avatar-qa.html'),'utf8'),checklist=fs.readFileSync(path.join(root,'concierge-avatar-checklist.html'),'utf8');
  assert.match(studio,/concierge-avatar-qa\.js/);assert.match(studio,/Static fallback confirmed\. GPU rendering, shadows and FPS were not tested\./);assert.match(studio,/BritesAvatarAcceptance\.collect/);
  for(const term of ['vendor','renderer','shadowMap','textures','pixelRatio','frameSampling','reduced-motion','contextLoss','unverified'])assert.match(checklist,new RegExp(term,'i'),term);
  assert.match(checklist,/never changes browser graphics settings or fingerprinting/i);assert.match(checklist,/never forces a context loss/i);
});
