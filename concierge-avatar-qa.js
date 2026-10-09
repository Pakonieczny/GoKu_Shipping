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
  const percentile=(values,ratio)=>{
    const ordered=(Array.isArray(values)?values:[]).filter(Number.isFinite).sort((a,b)=>a-b);
    if(!ordered.length)return null;
    const position=(ordered.length-1)*Math.max(0,Math.min(1,ratio)),lower=Math.floor(position),upper=Math.ceil(position);
    return rounded(ordered[lower]+(ordered[upper]-ordered[lower])*(position-lower));
  };
  function performanceSummary(input={}){
    const fps=(Array.isArray(input.fps)?input.fps:[]).filter(Number.isFinite),renderMs=(Array.isArray(input.renderMs)?input.renderMs:[]).filter(Number.isFinite),longTasks=input.longTasks||{};
    return {
      measuredMs:finite(input.measuredMs,0,120000),sampleCount:Math.max(fps.length,renderMs.length),medianFps:percentile(fps,.5),p95Fps:percentile(fps,.95),medianFrameRenderMs:percentile(renderMs,.5),p95FrameRenderMs:percentile(renderMs,.95),
      firstFrame:{status:boundedText(input.firstFrame?.status,48)||'not_observed',milliseconds:finite(input.firstFrame?.milliseconds,0,60000),relativeTo:boundedText(input.firstFrame?.relativeTo,48)||'sample_start'},
      longTasks:{supported:longTasks.supported===true,count:finite(longTasks.count,0,100000)||0,totalMs:finite(longTasks.totalMs,0,3600000)||0,maxMs:finite(longTasks.maxMs,0,3600000)||0,thresholdMs:50}
    };
  }
  function buildDiagnostics(input={}){
    const before=input.before||{},after=input.after||{},gl=input.gl||{},display=input.display||{},sampleMs=Math.max(1,number(input.sampleMs)||15000),performance=performanceSummary(input.performance);
    const mode=after.mode||before.mode||'pending',contextLost=after.contextLost===true||gl.contextLost===true,webglAvailable=gl.available===true,webglUsable=mode==='webgl'&&webglAvailable&&!contextLost;
    const shadow=after.shadow||{},requestedShadow=number(shadow.size),maxTexture=number(gl.maxTextureSize),shadowCapable=webglUsable&&shadow.enabled===true&&shadow.casts===true&&shadow.receivingStage===true&&requestedShadow!=null&&maxTexture!=null&&requestedShadow<=maxTexture&&number(gl.depthBits)>0;
    const observedTextures=Array.isArray(after.textures)?after.textures:[],moduleDeclarations=Array.isArray(after.declarations?.textures)?after.declarations.textures:[],declaredTextures=moduleDeclarations.length?moduleDeclarations:observedTextures,declarationSource=moduleDeclarations.length?'loaded_scene_module':observedTextures.length?'renderer_snapshot':'unavailable',declaredTextureSize=declaredTextures.reduce((largest,item)=>Math.max(largest,number(item?.width)||0,number(item?.height)||0),0)||number(after.quality?.textureSize);
    const textureFits=webglUsable&&declaredTextureSize!=null&&maxTexture!=null?declaredTextureSize<=maxTexture:null;
    const framesBefore=number(before.frames),framesAfter=number(after.frames),frameDelta=framesBefore!=null&&framesAfter!=null?Math.max(0,framesAfter-framesBefore):null,effectiveFps=frameDelta==null?null:rounded(frameDelta*1000/(performance.measuredMs||sampleMs));
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
      frameSampling:{status:frameStatus,sampleMs,measuredMs:performance.measuredMs,framesBefore,framesAfter,frameDelta,effectiveFps,frameRenderMs:number(after.frameRenderMs),targetFps:number(after.quality?.fps),sampleCount:performance.sampleCount,medianFps:performance.medianFps,p95Fps:performance.p95Fps,medianFrameRenderMs:performance.medianFrameRenderMs,p95FrameRenderMs:performance.p95FrameRenderMs,firstFrame:performance.firstFrame},
      longTasks:performance.longTasks,
      motion:{visible,intersecting,reducedMotion,paused:after.paused===true,animated:after.animated===true},
      contextLoss:{status:contextLost?'lost_fallback_required':webglUsable?'not_observed':'unverified_no_webgl_context',reportedByScene:after.contextLost===true,reportedByWebGL:gl.contextLost===true,forcedForTest:false},
      fallback:{kind:after.fallback?.format||'animated_svg_2d',animated:after.fallback?.animated===true,gpu:false},
      staticFallback:{active:fallbackActive,visible:fallbackActive&&input.frameHidden!==true,preserved:fallbackActive&&input.frameHidden!==true},
      acceptance:{gpu:webglUsable?'capability_observed_visual_unverified':'unverified',shadows:shadowCapable?'capability_observed_visual_unverified':'unverified',fps:frameStatus==='sampled'?'sampled':'unverified',message:webglUsable?'WebGL capability is visible below. GPU appearance and shadow quality still require visual review.':fallbackActive?'Animated 2-D fallback is active. GPU rendering, real-time shadows and FPS are unverified in this browser.':'Renderer capability is pending; GPU rendering, shadows and FPS are not yet verified.'}
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
  async function collect({avatar,win=scope,sampleMs=15000,sampleIntervalMs=250,readyWaitMs=2500,knownFirstFrameMs=null}={}){
    if(!avatar||typeof avatar.snapshot!=='function')throw Error('Avatar diagnostics require the QA avatar instance.');
    sampleMs=Math.max(1,Math.min(60000,number(sampleMs)||15000));sampleIntervalMs=Math.max(50,Math.min(1000,number(sampleIntervalMs)||250));
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
    const before=avatar.snapshot(),frame=avatar.element,gl=inspectWebGL(frame),canvas=frame?.querySelector?.('canvas'),box=canvas?.getBoundingClientRect?.()||{},fps=[],renderMs=[];
    const baselineFrames=number(before.frames),longTasks={supported:false,count:0,totalMs:0,maxMs:0},clock=()=>number(win?.performance?.now?.())??Date.now();let observer=null,elapsed=0,measuredMs=0,previous=before,previousAt=clock(),firstObservedMs=baselineFrames>0?0:null;
    if(typeof win?.PerformanceObserver==='function')try{
      observer=new win.PerformanceObserver(list=>{for(const entry of list.getEntries?.()||[])if(number(entry?.duration)>=50){longTasks.count++;longTasks.totalMs+=entry.duration;longTasks.maxMs=Math.max(longTasks.maxMs,entry.duration);}});
      observer.observe({type:'longtask',buffered:false});longTasks.supported=true;
    }catch(error){observer=null;}
    try{
      while(elapsed<sampleMs){const step=Math.min(sampleIntervalMs,sampleMs-elapsed);await wait(win,step);elapsed+=step;const observedAt=clock(),observedStep=Math.max(1,observedAt-previousAt)||step,actualStep=observedStep===1&&observedAt===previousAt?step:observedStep,current=avatar.snapshot()||{},priorFrames=number(previous.frames),currentFrames=number(current.frames),delta=priorFrames!=null&&currentFrames!=null?Math.max(0,currentFrames-priorFrames):null;measuredMs+=actualStep;if(delta!=null)fps.push(delta*1000/actualStep);if(number(current.frameRenderMs)!=null)renderMs.push(current.frameRenderMs);if(firstObservedMs==null&&baselineFrames!=null&&currentFrames!=null&&currentFrames>baselineFrames)firstObservedMs=measuredMs;previous=current;previousAt=observedAt;}
    }finally{observer?.disconnect?.();}
    longTasks.totalMs=rounded(longTasks.totalMs)||0;longTasks.maxMs=rounded(longTasks.maxMs)||0;
    const after=previous||avatar.snapshot(),media=win?.matchMedia?.('(prefers-reduced-motion: reduce)'),known=finite(knownFirstFrameMs,0,60000),firstFrame=known!=null?{status:'ready_event_observed',milliseconds:known,relativeTo:'avatar_create'}:firstObservedMs===0?{status:'already_rendered_before_sample',milliseconds:0,relativeTo:'sample_start'}:firstObservedMs!=null?{status:'observed_during_sample',milliseconds:firstObservedMs,relativeTo:'sample_start'}:{status:'not_observed',milliseconds:null,relativeTo:'sample_start'};
    return {...buildDiagnostics({before,after,gl,sampleMs,readiness,reducedMotion:media?.matches===true,frameRendering:frame?.dataset?.rendering,frameHidden:frame?.hidden===true,display:{devicePixelRatio:number(win?.devicePixelRatio),cssWidth:number(box.width),bufferWidth:number(canvas?.width)},performance:{fps,renderMs,measuredMs,firstFrame,longTasks}}),collectedAt:new Date().toISOString()};
  }
  async function exerciseStates({avatar,win=scope,settleMs=120}={}){
    if(!avatar||typeof avatar.snapshot!=='function'||typeof avatar.setState!=='function')return{status:'unavailable',visualAppearance:'unverified',states:[],gestures:[]};
    settleMs=Math.max(32,Math.min(1000,number(settleMs)||120));
    const opening=avatar.snapshot()||{};
    if(opening.visible===false||opening.paused===true)return{status:'blocked_avatar_not_active',visualAppearance:'unverified',states:[],gestures:[]};
    const original=state(opening.state)||'idle',rows=[];
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
  function auditCpuPoses({poseFor,states=STATES}={}){
    if(typeof poseFor!=='function')return{status:'unavailable',evidenceLevel:'cpu_only_visual_unverified',checks:{},states:[],claims:{expressionAppearance:'unverified'}};
    const requested=(Array.isArray(states)?states:[]).filter(value=>STATES.includes(value));
    const rows=requested.map(requestedState=>{
      const pose=poseFor({state:requestedState,time:7.25,elapsed:.6,level:.8,gaze:{x:.2,y:-.1}})||{};
      const numeric=Object.entries(pose).filter(([,value])=>typeof value==='number');
      return{state:requestedState,observedState:state(pose.state),finite:numeric.every(([,value])=>Number.isFinite(value)),eyeColor:boundedText(pose.eyeColor,24),eyeOpen:finite(pose.eyeOpen,0,2),eyeDeformation:finite(pose.eyeDeformation,-2,2),faceExpression:boundedText(pose.faceExpression,32),faceBrowLift:finite(pose.faceBrowLift,-1,1),faceBrowTilt:finite(pose.faceBrowTilt,-1,1),smileCurve:finite(pose.smileCurve,0,1),browLift:finite(pose.browLift,-2,2),gazeY:finite(pose.gazeY,-2,2),mouth:boundedText(pose.mouth,24),mouthOpen:finite(pose.mouthOpen,0,2),armLift:finite(pose.armLift,0,2),bob:finite(pose.bob,-2,2),headRoll:finite(pose.headRoll,-2,2)};
    });
    const byState=Object.fromEntries(rows.map(row=>[row.state,row])),idle=byState.idle||{};
    const reducedStable=requested.every(requestedState=>{
      const first=poseFor({state:requestedState,time:1,elapsed:1,level:.8,reducedMotion:true}),later=poseFor({state:requestedState,time:101,elapsed:101,level:.8,reducedMotion:true});
      return JSON.stringify(first)===JSON.stringify(later);
    });
    const silent=poseFor({state:'speaking',level:0,time:7.25,elapsed:7.25})||{},idleLater=poseFor({state:'idle',time:101,elapsed:101,level:.8,gaze:{x:.2,y:-.1}})||{};
    const sounding=poseFor({state:'speaking',level:.8,time:7.25,elapsed:7.25})||{};
    const checks={allStates:rows.length===STATES.length&&STATES.every(value=>byState[value]?.observedState===value),finite:rows.every(row=>row.finite),bounded:rows.every(row=>row.eyeOpen!=null&&row.eyeOpen>=.035&&row.eyeOpen<=1.1&&row.mouthOpen===0&&row.faceBrowLift!=null&&row.faceBrowTilt!=null&&row.smileCurve!=null),distinctStateColours:new Set(rows.map(row=>row.eyeColor)).size===rows.length,distinctFaceShapes:new Set(rows.map(row=>row.faceExpression)).size===rows.length,listeningCue:(byState.listening?.faceBrowLift||0)>(idle.faceBrowLift||0),thinkingCue:byState.thinking?.faceExpression==='curious'&&Math.abs(byState.thinking.faceBrowTilt)<=.02&&byState.thinking.faceBrowLift>idle.faceBrowLift&&byState.thinking.smileCurve>.28,speakingCue:byState.speaking?.mouth==='smile-curve'&&sounding.mouthOpen===0&&Number.isFinite(sounding.mouthCurve)&&sounding.speechEnergy===.8,silentOutputRests:silent.mouthOpen===0&&silent.speechEnergy===0&&silent.phraseGesture===0,successCue:(byState.success?.smileCurve||0)>(idle.smileCurve||0),errorCue:byState.error?.faceExpression==='reassuring',stablePointerDirection:idleLater.gazeY===idle.gazeY,reducedMotionDeterministic:reducedStable};
    const passed=Object.values(checks).every(Boolean);
    return{status:passed?'cpu_pose_contract_observed_visual_unverified':'cpu_pose_contract_not_confirmed',evidenceLevel:'cpu_only_visual_unverified',checks,states:rows,claims:{expressionAppearance:'unverified',gestureAppearance:'unverified'}};
  }
  function auditSceneSource({sceneSource='',controllerSource=''}={}){
    sceneSource=typeof sceneSource==='string'?sceneSource:'';controllerSource=typeof controllerSource==='string'?controllerSource:'';
    // A runtime receiver value may be computed from visible scene objects.
    // Minified snapshot labels alone cannot establish the actual declarations.
    const compiled=/shadow:\{enabled:!0,[^}]*type:"PCF soft",casts:!0,receivingStage:[^}]+\}/.test(sceneSource)&&/materials:\{physical:[^}]*metallicAnisotropy:!0,[^}]*environmentReflection:!0\}/.test(sceneSource)&&/\.shadowMap\.enabled=!0/.test(sceneSource)&&/([\w$]+)\.castShadow=!0,\1\.shadow\.mapSize\.set\([\w$]+\.shadowSize,[\w$]+\.shadowSize\)/.test(sceneSource)&&/([\w$]+)\.receiveShadow=!0,\1\.name="ground-shadow-receiver"/.test(sceneSource);
    const checks={physicalMaterials:/new THREE\.MeshPhysicalMaterial\s*\(|isMeshPhysicalMaterial/.test(sceneSource),declaredTextureInventory:/textures:\s*Object\.freeze\s*\(\s*\[/.test(sceneSource)&&/tangent-space normal/.test(sceneSource),generatedPbrMaps:/normalMap\s*:|normalMap\s*,/.test(sceneSource)&&/roughnessMap/.test(sceneSource)&&/bumpMap/.test(sceneSource),clearcoat:/clearcoat\s*:/.test(sceneSource),transmission:/transmission\s*:/.test(sceneSource),metallicAnisotropy:/anisotropy\s*:|metallicAnisotropy:!0/.test(sceneSource),environmentReflection:/scene\.environment\s*=\s*environmentTarget\.texture/.test(sceneSource)||compiled,shadowMapEnabled:/shadowMap\.enabled\s*=\s*true/.test(sceneSource)||compiled,softShadowDeclaration:/shadowMap\.type\s*=\s*THREE\.PCFSoftShadowMap/.test(sceneSource)||compiled,castingLight:/key\.castShadow\s*=\s*true/.test(sceneSource)||compiled,shadowMapBounded:/key\.shadow\.mapSize\.set\s*\(\s*quality\.shadowSize/.test(sceneSource)||compiled,receivingGeometry:/receiveShadow\s*=\s*true/.test(sceneSource)&&/const floor\s*=\s*mesh\(/.test(sceneSource)||compiled,stateDeformation:/attribute\.setXYZ\s*\(/.test(sceneSource)&&/apertureGeometry\.computeVertexNormals\s*\(/.test(sceneSource)||/meshDeformation:!0/.test(sceneSource)&&/computeVertexNormals/.test(sceneSource),pauseControl:/function setPaused\s*\(/.test(controllerSource)&&/engine\.setMotion\s*\(/.test(controllerSource),fallbackControl:/animated_svg_2d/.test(controllerSource)&&/renderingFailure/.test(controllerSource)};
    const passed=Object.values(checks).every(Boolean);
    return{status:passed?'source_contract_declared_visual_unverified':'source_contract_incomplete',evidenceLevel:'source_only_visual_unverified',checks,claims:{gpuAppearance:'unverified',shadowAppearance:'unverified',materialAppearance:'unverified',expressionAppearance:'unverified'}};
  }
  async function exerciseDomControls({avatar,win=scope,settleMs=40}={}){
    if(!avatar||typeof avatar.snapshot!=='function'||typeof avatar.setState!=='function'||typeof avatar.setPaused!=='function'||!avatar.element?.querySelector)return{status:'unavailable',evidenceLevel:'dom_only_visual_unverified',checks:{},states:[],claims:{expressionAppearance:'unverified'}};
    settleMs=Math.max(16,Math.min(500,number(settleMs)||40));
    const opening=avatar.snapshot()||{},frame=avatar.element;
    if(opening.destroyed===true||opening.visible===false)return{status:'blocked_avatar_not_active',evidenceLevel:'dom_only_visual_unverified',checks:{},states:[],claims:{expressionAppearance:'unverified'}};
    const originalState=state(opening.state)||'idle',originalPaused=opening.paused===true,rows=[];
    try{
      if(originalPaused)avatar.setPaused(false);
      for(const requestedState of STATES){avatar.setState(requestedState);await wait(win,settleMs);const snap=avatar.snapshot()||{};rows.push({state:requestedState,observedState:state(snap.state),datasetState:state(frame.dataset?.state),ariaLabelPresent:!!boundedText(frame.getAttribute?.('aria-label'),240),fallbackVectorPresent:!!frame.querySelector('.brites-avatar__fallback svg')});}
      avatar.setPaused(true);await wait(win,settleMs);
      const paused=avatar.snapshot()||{},pauseMotion=boundedText(frame.dataset?.motion,24),fallbackVector=frame.querySelector('.brites-avatar__fallback svg'),fallbackActive=paused.mode==='fallback';
      const checks={stateDatasetSynchronized:rows.every(row=>row.observedState===row.state&&row.datasetState===row.state),accessibleStatus:rows.every(row=>row.ariaLabelPresent),fallbackVectorPreserved:rows.every(row=>row.fallbackVectorPresent)&&!!fallbackVector,pauseSnapshot:paused.paused===true,pauseDomSignal:pauseMotion==='paused'||pauseMotion==='reduced',pauseStopsAnimation:paused.animated!==true,fallbackTruthful:!fallbackActive||(paused.fallback?.format==='animated_svg_2d'&&frame.dataset?.rendering==='fallback'),fallbackPaused:!fallbackActive||paused.fallback?.animated===false};
      const passed=Object.values(checks).every(Boolean);
      return{status:passed?'dom_state_pause_fallback_contract_observed_visual_unverified':'dom_state_pause_fallback_contract_not_confirmed',evidenceLevel:'dom_only_visual_unverified',checks,states:rows,mode:boundedText(paused.mode,24),claims:{gpuAppearance:'unverified',shadowAppearance:'unverified',expressionAppearance:'unverified',fallbackAppearance:'unverified'}};
    }finally{avatar.setPaused(originalPaused);avatar.setState(originalState);}
  }
  function acceptancePackage({diagnostics={},stateEvidence={}}={}){
    const webgl=diagnostics.webgl||{},runtime=diagnostics.runtime||{},shadow=diagnostics.shadowMap||{},textures=diagnostics.textures||{},frames=diagnostics.frameSampling||{},longTasks=diagnostics.longTasks||{},motion=diagnostics.motion||{},fallback=diagnostics.staticFallback||{},contextLoss=diagnostics.contextLoss||{},pixel=diagnostics.pixelRatio||{};
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
      frameSample:{status:boundedText(frames.status,64),sampleMs:finite(frames.sampleMs,0,60000),measuredMs:finite(frames.measuredMs,0,120000),framesBefore:finite(frames.framesBefore,0,1e9),framesAfter:finite(frames.framesAfter,0,1e9),frameDelta:finite(frames.frameDelta,0,1e8),effectiveFps:finite(frames.effectiveFps,0,10000),frameRenderMs:finite(frames.frameRenderMs,0,60000),targetFps:finite(frames.targetFps,0,1000),sampleCount:finite(frames.sampleCount,0,10000),medianFps:finite(frames.medianFps,0,10000),p95Fps:finite(frames.p95Fps,0,10000),medianFrameRenderMs:finite(frames.medianFrameRenderMs,0,60000),p95FrameRenderMs:finite(frames.p95FrameRenderMs,0,60000),firstFrame:{status:boundedText(frames.firstFrame?.status,48),milliseconds:finite(frames.firstFrame?.milliseconds,0,60000),relativeTo:boundedText(frames.firstFrame?.relativeTo,48)}},
      longTasks:{supported:longTasks.supported===true,count:finite(longTasks.count,0,100000)||0,totalMs:finite(longTasks.totalMs,0,3600000)||0,maxMs:finite(longTasks.maxMs,0,3600000)||0,thresholdMs:50},
      pixelRatio:{status:boundedText(pixel.status,64),device:finite(pixel.device,0,16),requested:finite(pixel.requested,0,16),renderer:finite(pixel.renderer,0,16),backingStore:finite(pixel.backingStore,0,16)},
      expressionsAndGestures:{status:boundedText(stateEvidence.status,80)||'not_exercised',visualAppearance:'unverified',states,gestures},
      pauseAndFallback:{visible:motion.visible===true,intersecting:motion.intersecting===true,reducedMotion:motion.reducedMotion===true,paused:motion.paused===true,animated:motion.animated===true,fallbackActive:fallback.active===true,fallbackVisible:fallback.visible===true,fallbackPreserved:fallback.preserved===true,contextLossStatus:boundedText(contextLoss.status,64)},
      claims:{gpuAppearance:'unverified',shadowAppearance:'unverified',expressionAppearance:'unverified',gestureAppearance:'unverified'}
    };
    return packageValue;
  }
  function serializeAcceptancePackage(input){
    const value=input?.kind==='brites_avatar_runtime_acceptance'?acceptancePackage({diagnostics:{collectedAt:input.collectedAt,runtime:input.runtime,webgl:input.webgl,shadowMap:input.shadows,textures:input.textures,frameSampling:input.frameSample,longTasks:input.longTasks,pixelRatio:input.pixelRatio,motion:input.pauseAndFallback,staticFallback:{active:input.pauseAndFallback?.fallbackActive,visible:input.pauseAndFallback?.fallbackVisible,preserved:input.pauseAndFallback?.fallbackPreserved},contextLoss:{status:input.pauseAndFallback?.contextLossStatus}},stateEvidence:input.expressionsAndGestures}):acceptancePackage(input);
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
  const api={buildDiagnostics,inspectWebGL,collect,exerciseStates,auditCpuPoses,auditSceneSource,exerciseDomControls,acceptancePackage,serializeAcceptancePackage,collectAcceptancePackage,copyAcceptancePackage,downloadAcceptancePackage,performanceSummary};
  if(typeof module!=='undefined'&&module.exports)module.exports=api;
  if(scope)scope.BritesAvatarAcceptance=api;
})(typeof window==='undefined'?null:window);
