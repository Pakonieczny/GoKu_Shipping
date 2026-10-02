(function(scope){
  'use strict';
  const number=value=>Number.isFinite(value)?value:null;
  const rounded=value=>Number.isFinite(value)?Math.round(value*100)/100:null;
  const text=value=>typeof value==='string'&&value.trim()?value.trim():null;
  const STATES=Object.freeze(['idle','listening','thinking','speaking','success','error']);
  const MAX_EXPORT_BYTES=24576;
  const boundedText=(value,limit=160)=>{const clean=text(value)?.replace(/[\u0000-\u001f\u007f]/g,' ');return clean?clean.slice(0,limit):null;};
  const state=value=>STATES.includes(value)?value:null;
  const finite=(value,min=-1e9,max=1e9)=>{value=number(value);return value==null?null:Math.max(min,Math.min(max,value));};
  const wait=(win,ms)=>new Promise(resolve=>(win?.setTimeout||setTimeout)(resolve,ms));
  function buildDiagnostics(input={}){
    const before=input.before||{},after=input.after||{},gl=input.gl||{},display=input.display||{},sampleMs=Math.max(1,number(input.sampleMs)||750);
    const mode=after.mode||before.mode||'pending',contextLost=after.contextLost===true||gl.contextLost===true,webglAvailable=gl.available===true,webglUsable=mode==='webgl'&&webglAvailable&&!contextLost;
    const shadow=after.shadow||{},requestedShadow=number(shadow.size),maxTexture=number(gl.maxTextureSize),shadowCapable=webglUsable&&shadow.enabled===true&&shadow.casts===true&&shadow.receivingStage===true&&requestedShadow!=null&&maxTexture!=null&&requestedShadow<=maxTexture&&number(gl.depthBits)>0;
    const observedTextures=Array.isArray(after.textures)?after.textures:[],moduleDeclarations=Array.isArray(after.declarations?.textures)?after.declarations.textures:[],declaredTextures=moduleDeclarations.length?moduleDeclarations:observedTextures,declarationSource=moduleDeclarations.length?'loaded_scene_module':observedTextures.length?'renderer_snapshot':'unavailable',declaredTextureSize=declaredTextures.reduce((largest,item)=>Math.max(largest,number(item?.width)||0,number(item?.height)||0),0)||number(after.quality?.textureSize);
    const textureFits=webglUsable&&declaredTextureSize!=null&&maxTexture!=null?declaredTextureSize<=maxTexture:null;
    const framesBefore=number(before.frames),framesAfter=number(after.frames),frameDelta=framesBefore!=null&&framesAfter!=null?Math.max(0,framesAfter-framesBefore):null,effectiveFps=frameDelta==null?null:rounded(frameDelta*1000/sampleMs);
    const reducedMotion=after.reducedMotion===true||input.reducedMotion===true,visible=after.visible===true,intersecting=after.intersecting!==false;
    const frameStatus=!webglUsable?(contextLost?'unverified_context_lost':mode==='pending'?'unverified_renderer_pending':'unverified_webgl_unavailable'):reducedMotion?'intentionally_paused_reduced_motion':!visible||!intersecting?'paused_not_visible':frameDelta>0?'sampled':'no_frames_observed';
    const rendererRatio=number(after.pixelRatio),deviceRatio=number(display.devicePixelRatio),cssWidth=number(display.cssWidth),bufferWidth=number(display.bufferWidth),backingRatio=cssWidth>0&&bufferWidth!=null?rounded(bufferWidth/cssWidth):null;
    const fallbackActive=mode==='fallback'||input.frameRendering==='fallback';
    const unavailableReason=contextLost?'context_lost':mode==='pending'?'renderer_pending':webglAvailable?'scene_fallback':'webgl_unavailable';
    const readiness=input.readiness||{};
    return {
      schema:1,mode,
      runtime:{readiness:text(readiness.status)||'not_recorded',waitMs:number(readiness.waitMs),loading:after.loading===true,destroyed:after.destroyed===true,onDemandDeferred:(text(readiness.status)==='deferred_not_visible')},
      webgl:{status:webglUsable?'available':unavailableReason,contextType:text(gl.contextType),vendor:text(gl.vendor),renderer:text(gl.renderer),version:text(gl.version),shadingLanguageVersion:text(gl.shadingLanguageVersion),maxTextureSize:maxTexture,maxRenderbufferSize:number(gl.maxRenderbufferSize),depthBits:number(gl.depthBits),maxCombinedTextureUnits:number(gl.maxCombinedTextureUnits),depthTexture:gl.depthTexture===true,contextLost},
      gpu:{status:webglUsable?'capability_observed':'unverified_'+unavailableReason,visualQuality:'unverified'},
      shadowMap:{status:shadowCapable?'capability_observed_visual_unverified':webglUsable?'capability_not_confirmed':'unverified_'+unavailableReason,configured:shadow.enabled===true,casts:shadow.casts===true,receivingStage:shadow.receivingStage===true,type:text(shadow.type),requestedSize:requestedShadow,maxTextureSize:maxTexture,depthBits:number(gl.depthBits),capabilityObserved:shadowCapable,appearance:'unverified'},
      textures:{status:textureFits===true?'fits_observed_capability':textureFits===false?'exceeds_observed_capability':webglUsable?'capability_not_confirmed':'unverified_'+unavailableReason,configuredSize:number(after.quality?.textureSize),largestDeclaredSize:declaredTextureSize,declaredMaps:declaredTextures.length,observedCreatedMaps:observedTextures.length,declarationSource,maxTextureSize:maxTexture,fitsObservedCapability:textureFits},
      pixelRatio:{device:deviceRatio,requested:number(after.quality?.pixelRatio),renderer:rendererRatio,backingStore:backingRatio,status:webglUsable&&rendererRatio!=null?'observed':'unverified_'+unavailableReason},
      frameSampling:{status:frameStatus,sampleMs,framesBefore,framesAfter,frameDelta,effectiveFps,frameRenderMs:number(after.frameRenderMs),targetFps:number(after.quality?.fps)},
      motion:{visible,intersecting,reducedMotion,animated:after.animated===true},
      contextLoss:{status:contextLost?'lost_fallback_required':webglUsable?'not_observed':'unverified_no_webgl_context',reportedByScene:after.contextLost===true,reportedByWebGL:gl.contextLost===true,forcedForTest:false},
      staticFallback:{active:fallbackActive,visible:fallbackActive&&input.frameHidden!==true,preserved:fallbackActive&&input.frameHidden!==true},
      acceptance:{gpu:webglUsable?'capability_observed_visual_unverified':'unverified',shadows:shadowCapable?'capability_observed_visual_unverified':'unverified',fps:frameStatus==='sampled'?'sampled':'unverified',message:webglUsable?'WebGL capability is visible below. GPU appearance and shadow quality still require visual review.':fallbackActive?'Static fallback is active. GPU rendering, real-time shadows and FPS are unverified in this browser.':'Renderer capability is pending; GPU rendering, shadows and FPS are not yet verified.'}
    };
  }
  function inspectWebGL(frame){
    const canvas=frame?.querySelector?.('canvas');if(!canvas)return{available:false};
    let gl=null,contextType=null;
    for(const type of ['webgl2','webgl']){try{gl=canvas.getContext(type);if(gl){contextType=type;break;}}catch(e){}}
    if(!gl)return{available:false};
    let debug=null;try{debug=gl.getExtension('WEBGL_debug_renderer_info');}catch(e){}
    const parameter=value=>{try{return gl.getParameter(value);}catch(e){return null;}};
    const webgl2=contextType==='webgl2',depthTexture=webgl2||(()=>{try{return!!gl.getExtension('WEBGL_depth_texture');}catch(e){return false;}})();
    return {available:true,contextType,vendor:parameter(debug?.UNMASKED_VENDOR_WEBGL)||parameter(gl.VENDOR),renderer:parameter(debug?.UNMASKED_RENDERER_WEBGL)||parameter(gl.RENDERER),version:parameter(gl.VERSION),shadingLanguageVersion:parameter(gl.SHADING_LANGUAGE_VERSION),maxTextureSize:parameter(gl.MAX_TEXTURE_SIZE),maxRenderbufferSize:parameter(gl.MAX_RENDERBUFFER_SIZE),depthBits:parameter(gl.DEPTH_BITS),maxCombinedTextureUnits:parameter(gl.MAX_COMBINED_TEXTURE_IMAGE_UNITS),depthTexture,contextLost:typeof gl.isContextLost==='function'&&gl.isContextLost()};
  }
  async function collect({avatar,win=scope,sampleMs=750,readyWaitMs=2500}={}){
    if(!avatar||typeof avatar.snapshot!=='function')throw Error('Avatar diagnostics require the QA avatar instance.');
    readyWaitMs=Math.max(0,Math.min(10000,number(readyWaitMs)??2500));
    const initial=avatar.snapshot()||{};let readiness={status:'already_settled',waitMs:0};
    if(initial.destroyed===true)readiness={status:'destroyed',waitMs:0};
    else if(initial.mode==='pending'&&initial.visible===false&&initial.loading!==true)readiness={status:'deferred_not_visible',waitMs:0};
    else if(initial.mode==='pending'&&avatar.ready&&typeof avatar.ready.then==='function'){
      const started=Date.now();let timer=null;
      try{
        const result=await Promise.race([Promise.resolve(avatar.ready).then(()=> 'settled',()=> 'rejected'),new Promise(resolve=>{timer=(win?.setTimeout||setTimeout)(()=>resolve('timed_out'),readyWaitMs);})]);
        readiness={status:result,waitMs:Math.max(0,Date.now()-started)};
      }finally{if(timer!=null)(win?.clearTimeout||clearTimeout)(timer);}
    }
    const before=avatar.snapshot(),frame=avatar.element,gl=inspectWebGL(frame),canvas=frame?.querySelector?.('canvas'),box=canvas?.getBoundingClientRect?.()||{};
    await new Promise(resolve=>(win?.setTimeout||setTimeout)(resolve,sampleMs));
    const after=avatar.snapshot(),media=win?.matchMedia?.('(prefers-reduced-motion: reduce)');
    return {...buildDiagnostics({before,after,gl,sampleMs,readiness,reducedMotion:media?.matches===true,frameRendering:frame?.dataset?.rendering,frameHidden:frame?.hidden===true,display:{devicePixelRatio:number(win?.devicePixelRatio),cssWidth:number(box.width),bufferWidth:number(canvas?.width)}}),collectedAt:new Date().toISOString()};
  }
  async function exerciseStates({avatar,win=scope,settleMs=120}={}){
    if(!avatar||typeof avatar.snapshot!=='function'||typeof avatar.setState!=='function')return{status:'unavailable',visualAppearance:'unverified',states:[],gestures:[]};
    settleMs=Math.max(32,Math.min(1000,number(settleMs)||120));
    const original=state(avatar.snapshot()?.state)||'idle',rows=[];
    try{
      for(const requestedState of STATES){
        const before=avatar.snapshot()||{},framesBefore=number(before.frames);
        avatar.setState(requestedState);await wait(win,settleMs);
        const after=avatar.snapshot()||{},framesAfter=number(after.frames),frameDelta=framesBefore!=null&&framesAfter!=null?Math.max(0,framesAfter-framesBefore):null,webgl=after.mode==='webgl'&&after.contextLost!==true;
        rows.push({state:requestedState,observedState:state(after.state),mode:after.mode==='webgl'?'webgl':after.mode==='fallback'?'fallback':'pending',frameDelta,renderObserved:webgl&&frameDelta>0});
      }
    }finally{avatar.setState(original);}
    const allAccepted=rows.length===STATES.length&&rows.every(row=>row.observedState===row.state),allRendered=allAccepted&&rows.every(row=>row.renderObserved);
    return {status:allRendered?'state_pipeline_and_frames_observed_visual_unverified':allAccepted?'state_pipeline_observed_render_unverified':'state_pipeline_not_confirmed',visualAppearance:'unverified',states:rows,gestures:[{name:'speaking_arm_motion',state:'speaking',renderObserved:rows.find(row=>row.state==='speaking')?.renderObserved===true},{name:'success_arm_lift',state:'success',renderObserved:rows.find(row=>row.state==='success')?.renderObserved===true}]};
  }
  function acceptancePackage({diagnostics={},stateEvidence={}}={}){
    const webgl=diagnostics.webgl||{},runtime=diagnostics.runtime||{},shadow=diagnostics.shadowMap||{},textures=diagnostics.textures||{},frames=diagnostics.frameSampling||{},motion=diagnostics.motion||{},fallback=diagnostics.staticFallback||{},contextLoss=diagnostics.contextLoss||{},pixel=diagnostics.pixelRatio||{};
    const webglObserved=webgl.status==='available',stateRows=Array.isArray(stateEvidence.states)?stateEvidence.states.slice(0,STATES.length):[];
    const states=stateRows.map(row=>({state:state(row?.state),observedState:state(row?.observedState),mode:['webgl','fallback','pending'].includes(row?.mode)?row.mode:'pending',frameDelta:finite(row?.frameDelta,0,1000000),renderObserved:row?.renderObserved===true})).filter(row=>row.state);
    const gestures=(Array.isArray(stateEvidence.gestures)?stateEvidence.gestures:[]).slice(0,4).map(item=>({name:['speaking_arm_motion','success_arm_lift'].includes(item?.name)?item.name:null,state:state(item?.state),renderObserved:item?.renderObserved===true})).filter(item=>item.name&&item.state);
    const packageValue={
      schema:1,kind:'brites_avatar_runtime_acceptance',sanitized:true,collectedAt:boundedText(diagnostics.collectedAt,40),
      evidenceLevel:webglObserved?'webgl_runtime_observed_visual_review_required':fallback.active===true?'fallback_only_webgl_unverified':'renderer_pending_unverified',
      runtime:{readiness:boundedText(runtime.readiness,48),waitMs:finite(runtime.waitMs,0,10000),loading:runtime.loading===true,destroyed:runtime.destroyed===true,onDemandDeferred:runtime.onDemandDeferred===true},
      webgl:{status:boundedText(webgl.status,48),contextType:boundedText(webgl.contextType,32),vendor:boundedText(webgl.vendor),renderer:boundedText(webgl.renderer),version:boundedText(webgl.version),shadingLanguageVersion:boundedText(webgl.shadingLanguageVersion),maxTextureSize:finite(webgl.maxTextureSize,0,1000000),maxRenderbufferSize:finite(webgl.maxRenderbufferSize,0,1000000),depthBits:finite(webgl.depthBits,0,128),maxCombinedTextureUnits:finite(webgl.maxCombinedTextureUnits,0,100000),depthTexture:webgl.depthTexture===true,contextLost:webgl.contextLost===true},
      shadows:{status:boundedText(shadow.status,64),configured:shadow.configured===true,casts:shadow.casts===true,receivingStage:shadow.receivingStage===true,type:boundedText(shadow.type,48),requestedSize:finite(shadow.requestedSize,0,1000000),capabilityObserved:shadow.capabilityObserved===true,appearance:'unverified'},
      textures:{status:boundedText(textures.status,64),configuredSize:finite(textures.configuredSize,0,1000000),largestDeclaredSize:finite(textures.largestDeclaredSize,0,1000000),declaredMaps:finite(textures.declaredMaps,0,64),observedCreatedMaps:finite(textures.observedCreatedMaps,0,64),declarationSource:boundedText(textures.declarationSource,48),maxTextureSize:finite(textures.maxTextureSize,0,1000000),fitsObservedCapability:textures.fitsObservedCapability===true?true:textures.fitsObservedCapability===false?false:null},
      frameSample:{status:boundedText(frames.status,64),sampleMs:finite(frames.sampleMs,0,60000),framesBefore:finite(frames.framesBefore,0,1e9),framesAfter:finite(frames.framesAfter,0,1e9),frameDelta:finite(frames.frameDelta,0,1e8),effectiveFps:finite(frames.effectiveFps,0,10000),frameRenderMs:finite(frames.frameRenderMs,0,60000),targetFps:finite(frames.targetFps,0,1000)},
      pixelRatio:{status:boundedText(pixel.status,64),device:finite(pixel.device,0,16),requested:finite(pixel.requested,0,16),renderer:finite(pixel.renderer,0,16),backingStore:finite(pixel.backingStore,0,16)},
      expressionsAndGestures:{status:boundedText(stateEvidence.status,80)||'not_exercised',visualAppearance:'unverified',states,gestures},
      pauseAndFallback:{visible:motion.visible===true,intersecting:motion.intersecting===true,reducedMotion:motion.reducedMotion===true,animated:motion.animated===true,fallbackActive:fallback.active===true,fallbackVisible:fallback.visible===true,fallbackPreserved:fallback.preserved===true,contextLossStatus:boundedText(contextLoss.status,64)},
      claims:{gpuAppearance:'unverified',shadowAppearance:'unverified',expressionAppearance:'unverified',gestureAppearance:'unverified'}
    };
    return packageValue;
  }
  function serializeAcceptancePackage(input){
    const value=input?.kind==='brites_avatar_runtime_acceptance'?acceptancePackage({diagnostics:{collectedAt:input.collectedAt,runtime:input.runtime,webgl:input.webgl,shadowMap:input.shadows,textures:input.textures,frameSampling:input.frameSample,pixelRatio:input.pixelRatio,motion:input.pauseAndFallback,staticFallback:{active:input.pauseAndFallback?.fallbackActive,visible:input.pauseAndFallback?.fallbackVisible,preserved:input.pauseAndFallback?.fallbackPreserved},contextLoss:{status:input.pauseAndFallback?.contextLossStatus}},stateEvidence:input.expressionsAndGestures}):acceptancePackage(input);
    const json=JSON.stringify(value,null,2),bytes=typeof TextEncoder==='function'?new TextEncoder().encode(json).length:json.length;
    if(bytes>MAX_EXPORT_BYTES)throw Error('Avatar acceptance package exceeds its safe export bound.');
    return{value,json,bytes};
  }
  async function collectAcceptancePackage(options={}){
    const diagnostics=await collect(options),stateEvidence=await exerciseStates(options);
    return acceptancePackage({diagnostics,stateEvidence});
  }
  async function copyAcceptancePackage(input,{navigator:nav=scope?.navigator}={}){
    const exported=serializeAcceptancePackage(input);
    if(typeof nav?.clipboard?.writeText!=='function')throw Error('Clipboard writing is unavailable.');
    await nav.clipboard.writeText(exported.json);return{copied:true,bytes:exported.bytes};
  }
  function downloadAcceptancePackage(input,{document:doc=scope?.document,URL:urlApi=scope?.URL,Blob:BlobCtor=scope?.Blob}={}){
    if(!doc?.createElement||!urlApi?.createObjectURL||typeof BlobCtor!=='function')throw Error('File download is unavailable.');
    const exported=serializeAcceptancePackage(input),url=urlApi.createObjectURL(new BlobCtor([exported.json],{type:'application/json'})),link=doc.createElement('a');
    link.href=url;link.download='brites-avatar-acceptance.json';link.rel='noopener';link.click();urlApi.revokeObjectURL(url);return{downloaded:true,bytes:exported.bytes,filename:link.download};
  }
  const api={buildDiagnostics,inspectWebGL,collect,exerciseStates,acceptancePackage,serializeAcceptancePackage,collectAcceptancePackage,copyAcceptancePackage,downloadAcceptancePackage};
  if(typeof module!=='undefined'&&module.exports)module.exports=api;
  if(scope)scope.BritesAvatarAcceptance=api;
})(typeof window==='undefined'?null:window);
