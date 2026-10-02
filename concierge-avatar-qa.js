(function(scope){
  'use strict';
  const number=value=>Number.isFinite(value)?value:null;
  const rounded=value=>Number.isFinite(value)?Math.round(value*100)/100:null;
  const text=value=>typeof value==='string'&&value.trim()?value.trim():null;
  function buildDiagnostics(input={}){
    const before=input.before||{},after=input.after||{},gl=input.gl||{},display=input.display||{},sampleMs=Math.max(1,number(input.sampleMs)||750);
    const mode=after.mode||before.mode||'pending',contextLost=after.contextLost===true||gl.contextLost===true,webglAvailable=gl.available===true,webglUsable=mode==='webgl'&&webglAvailable&&!contextLost;
    const shadow=after.shadow||{},requestedShadow=number(shadow.size),maxTexture=number(gl.maxTextureSize),shadowCapable=webglUsable&&shadow.enabled===true&&shadow.casts===true&&shadow.receivingStage===true&&requestedShadow!=null&&maxTexture!=null&&requestedShadow<=maxTexture&&number(gl.depthBits)>0;
    const observedTextures=Array.isArray(after.textures)?after.textures:[],moduleDeclarations=Array.isArray(after.declarations?.textures)?after.declarations.textures:[],declaredTextures=moduleDeclarations.length?moduleDeclarations:observedTextures,declarationSource=moduleDeclarations.length?'loaded_scene_module':observedTextures.length?'renderer_snapshot':'unavailable',declaredTextureSize=declaredTextures.reduce((largest,item)=>Math.max(largest,number(item?.width)||0,number(item?.height)||0),0)||number(after.quality?.textureSize);
    const textureFits=webglUsable&&declaredTextureSize!=null&&maxTexture!=null?declaredTextureSize<=maxTexture:null;
    const framesBefore=number(before.frames),framesAfter=number(after.frames),frameDelta=framesBefore!=null&&framesAfter!=null?Math.max(0,framesAfter-framesBefore):null,effectiveFps=frameDelta==null?null:rounded(frameDelta*1000/sampleMs);
    const reducedMotion=after.reducedMotion===true||input.reducedMotion===true,visible=after.visible===true,intersecting=after.intersecting!==false;
    const frameStatus=!webglUsable?(contextLost?'unverified_context_lost':'unverified_webgl_unavailable'):reducedMotion?'intentionally_paused_reduced_motion':!visible||!intersecting?'paused_not_visible':frameDelta>0?'sampled':'no_frames_observed';
    const rendererRatio=number(after.pixelRatio),deviceRatio=number(display.devicePixelRatio),cssWidth=number(display.cssWidth),bufferWidth=number(display.bufferWidth),backingRatio=cssWidth>0&&bufferWidth!=null?rounded(bufferWidth/cssWidth):null;
    const fallbackActive=mode==='fallback'||input.frameRendering==='fallback';
    const unavailableReason=contextLost?'context_lost':webglAvailable?'scene_fallback':'webgl_unavailable';
    return {
      schema:1,mode,
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
  async function collect({avatar,win=scope,sampleMs=750}={}){
    if(!avatar||typeof avatar.snapshot!=='function')throw Error('Avatar diagnostics require the QA avatar instance.');
    try{await avatar.ready;}catch(e){}
    const before=avatar.snapshot(),frame=avatar.element,gl=inspectWebGL(frame),canvas=frame?.querySelector?.('canvas'),box=canvas?.getBoundingClientRect?.()||{};
    await new Promise(resolve=>(win?.setTimeout||setTimeout)(resolve,sampleMs));
    const after=avatar.snapshot(),media=win?.matchMedia?.('(prefers-reduced-motion: reduce)');
    return {...buildDiagnostics({before,after,gl,sampleMs,reducedMotion:media?.matches===true,frameRendering:frame?.dataset?.rendering,frameHidden:frame?.hidden===true,display:{devicePixelRatio:number(win?.devicePixelRatio),cssWidth:number(box.width),bufferWidth:number(canvas?.width)}}),collectedAt:new Date().toISOString()};
  }
  const api={buildDiagnostics,inspectWebGL,collect};
  if(typeof module!=='undefined'&&module.exports)module.exports=api;
  if(scope)scope.BritesAvatarAcceptance=api;
})(typeof window==='undefined'?null:window);
