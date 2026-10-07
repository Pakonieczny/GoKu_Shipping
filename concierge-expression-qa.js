(function(scope){
  'use strict';
  // This route-only fixture uses authored media events and the production
  // expression planner/face rig. It cannot reserve inference or capture audio.
  const MAX_LOG=120, MAX_SAMPLES=180, MAX_RESULTS=16;
  const SCENARIOS=Object.freeze([
    {id:'phrases',label:'Warm opening → explanation → question',text:'Congratulations on her graduation. We can choose something personal. Which finish does she usually wear?',duration:12500,check:'multiple-cues',generationAhead:true},
    {id:'support',label:'Memorial support and gentle question',context:'My sister died. I want to remember her.',text:'I am sorry for your loss. We can take this slowly. Would you prefer a simple piece?',duration:13000,check:'support'},
    {id:'repair',label:'Frustration, acknowledgment and repair',context:'Those are not what I meant. This is frustrating.',text:'I missed what you were looking for. Let us narrow it down. What should I change first?',duration:12500,check:'multiple-cues'},
    {id:'mixed',label:'Birthday with a difficult week',context:'It is her birthday, but she is having a difficult week.',text:'We can make it thoughtful. A familiar symbol may feel comforting. Does she prefer playful or understated?',duration:13500,check:'multiple-cues'},
    {id:'uncertain',label:'Qualified meaning instead of certainty',text:'I cannot say it has one universal meaning. You might choose a personal reminder. What would you like it to represent?',duration:14500,check:'multiple-cues'},
    {id:'negation',label:'Negation and ambiguous context',context:'It is not a birthday gift. I am not confused.',text:'Thank you for clarifying. We can focus on the design. Which detail matters most to you?',duration:12500,check:'multiple-cues'},
    {id:'listen',label:'Listening before any transcript',role:'user',duration:7500,check:'listening-only'},
    {id:'listen-support',label:'Listening with actual available supportive text',role:'user',text:'I need a memorial gift for my sister.',duration:8500,check:'listening-only',transcriptAt:3400},
    {id:'silence',label:'Silence with no guessed emotion',role:'user',duration:6500,check:'silent',silence:true},
    {id:'interrupt',label:'Interrupt halfway through a speaking answer',text:'A small symbol can carry a personal story. We can compare the finishes. Which one would you like to see?',duration:10000,check:'interrupted',interruptAt:4200},
    {id:'pause',label:'Pause animation and resume without replay',text:'We can take this one step at a time. A simple detail may feel meaningful. Would you like to compare?',duration:12500,check:'pause',pauseAt:3400,resumeAt:6200},
    {id:'analysis',label:'Facial audio analysis unavailable',text:'We can still choose together. I will explain the details. Which shape feels right for them?',duration:12500,check:'analysis-unavailable',analysisUnavailable:true},
    {id:'hidden',label:'Hide the guide and return safely',text:'Let us look at the details. You can choose the meaning that feels personal. What do you think?',duration:12500,check:'hidden',hideAt:3400,showAt:6600},
    {id:'reduced',label:'Reduced motion during a multi-clause answer',text:'That is a lovely milestone. We can choose something personal. Which design would you like to compare?',duration:12500,check:'reduced',reduced:true},
    {id:'generation',label:'Generation ends while audio continues',text:'The finish changes the look of a piece. A small symbol may be a personal reminder. Which style feels right?',duration:14000,check:'multiple-cues',generationAhead:true},
    {id:'late',label:'Late old-response events after a new turn',text:'That sounds like a joyful milestone. We can find a personal design. Does she wear silver or gold?',duration:10500,check:'interrupted',interruptAt:4100,lateAt:5700}
  ].map(item=>Object.freeze(item)));
  const finite=value=>Number.isFinite(value)?value:null;
  const round=value=>Number.isFinite(value)?Math.round(value*1000)/1000:null;
  const clean=value=>typeof value==='string'?value.slice(0,400):'';
  function tapeFor(scenario){
    if(!SCENARIOS.includes(scenario))throw Error('Choose an authored scenario.');
    const role=scenario.role||'assistant',events=[{at:0,type:'begin',role}];
    if(role==='assistant'){
      events.push({at:80,type:'transcript',role:'assistant',text:scenario.text,final:true});
      // Text is deliberately available before output starts. No event is
      // represented as a provider-supplied word timestamp.
      events.push({at:900,type:'playback',playing:true},{at:scenario.duration-700,type:'playback',playing:false});
    }else{
      events.push({at:60,type:'listen',value:true});
      if(scenario.text)events.push({at:scenario.transcriptAt||3400,type:'transcript',role:'user',text:scenario.text,final:true});
      events.push({at:scenario.duration-500,type:'listen',value:false});
    }
    if(scenario.interruptAt)events.push({at:scenario.interruptAt,type:'interrupt'});
    if(scenario.pauseAt)events.push({at:scenario.pauseAt,type:'pause',value:true},{at:scenario.resumeAt,type:'pause',value:false});
    if(scenario.hideAt)events.push({at:scenario.hideAt,type:'hidden',value:true},{at:scenario.showAt,type:'hidden',value:false});
    if(scenario.lateAt)events.push({at:scenario.lateAt,type:'late'});
    events.push({at:scenario.duration,type:'finish'});
    return events.sort((a,b)=>a.at-b.at);
  }
  function faceSample(avatar){
    const state=avatar.snapshot(),frame=avatar.element,raw=state.facePose||state.pose||state.face||{};
    const attr=(name,key)=>frame?.querySelector('.brites-avatar__'+name)?.getAttribute(key)||'';
    const css=name=>frame?.style?.getPropertyValue(name)||'';
    const features={brows:[raw.faceBrowLift,raw.faceBrowTilt].filter(Number.isFinite).map(round).join(',')||attr('brow--left','d'),eyes:[raw.eyeScaleX,raw.eyeScaleY,raw.eyeSmile,raw.eyeOpen].filter(Number.isFinite).map(round).join(',')||attr('ribbon--left','d'),cheeks:Number.isFinite(raw.cheekGlow)?String(round(raw.cheekGlow)):attr('cheek--left','opacity'),smile:Number.isFinite(raw.smileCurve)?String(round(raw.smileCurve)):attr('smile-signal','d')};
    const aperture=finite(raw.speechEnergy)??finite(Number(css('--brites-speech-level')))??0;
    return {mode:state.mode,state:state.state,paused:state.paused===true,visible:state.visible===true,reducedMotion:state.reducedMotion===true,expression:state.expression?{kind:state.expression.kind,intensity:round(state.expression.intensity)}:null,emotion:state.emotion||null,features,mouthEnergy:round(aperture),mouthRy:round(Number(attr('speech-mouth','ry'))),mouthVisible:attr('speech-mouth','opacity')==='1',facePose:Object.fromEntries(Object.entries(raw).filter(([key,value])=>/^(?:faceBrow|eye|cheekGlow|smileCurve|head|speechEnergy)/.test(key)&&Number.isFinite(value)).map(([key,value])=>[key,round(value)]))};
  }
  function safeEvent(value){
    if(typeof value==='string')return {type:clean(value)};
    if(!value||typeof value!=='object')return {type:'expression-event'};
    const result={};for(const key of ['type','kind','intensity','index','phase','reason','timing','responseId','itemId','currentPhrase','cueIndex','queueLength','accepted','rejected'])if(['string','number','boolean'].includes(typeof value[key]))result[key]=typeof value[key]==='string'?clean(value[key]):value[key];
    if(value.cue?.kind)result.kind=clean(value.cue.kind);return result;
  }
  function createRunner({expressionFactory,avatar,onUpdate=()=>{},onComplete=()=>{},now=()=>0}={}){
    if(!expressionFactory?.create||!avatar?.setExpression)throw Error('The production expression planner and face rig are required.');
    // The authored source clock is separate from renderer updates. A sparse
    // browser update drains at most 50ms of source audio per planner tick, but
    // only its final pose is sent to the real rig and captured once.
    const SOURCE_STEP_MS=50,MAX_SOURCE_DRAINS=400;
    let clock=0,serial=0,controller=null,scenario=null,tape=[],next=0,nextSourceAt=0,running=false,manualPause=false,startAt=0,heldAt=0,playing=false,listening=false,interrupted=false,hidden=false,reduced=false,scriptedPaused=false,everPaused=false,everHidden=false,rigState='idle',pendingExpression=null,output=0,input=0,lastSampleAt=-Infinity,sourceSampleCount=0,sourcePlaybackSamples=0,observedUpdateCount=0,sourceBeforePlayback=[],sourceInterruptionChecks=[],sourceEventCoverage=[],sourcePhraseIndices=new Set(),sourceCueKinds=new Set(),finalInquiryReached=false,expectedFinalPhraseIndex=-1,samples=[],log=[],cues=[];
    function record(value){log.push({at:Math.round(clock),...safeEvent(value)});if(log.length>MAX_LOG)log.shift();}
    function applyExpression(cue){
      const value=cue&&typeof cue==='object'?{kind:cue.kind,intensity:cue.intensity}:null;
      if(value?.kind&&value.kind!=='neutral'){
        sourceCueKinds.add(value.kind);
        if(cues.at(-1)?.kind!==value.kind)cues.push({at:Math.round(clock),kind:clean(value.kind),intensity:round(value.intensity),evidence:'authored-source-target'});
      }
      if(cues.length>MAX_LOG)cues.shift();pendingExpression=value;
      record({type:'source-face-target',kind:value?.kind||'neutral',intensity:round(value?.intensity)});
    }
    function render(){
      avatar.setPaused(manualPause||scriptedPaused);avatar.setVisible(!hidden);avatar.setState(rigState);avatar.setExpression(pendingExpression);avatar.setLevel(output);
    }
    function reset(){
      controller?.destroy();controller=null;running=false;manualPause=false;playing=listening=interrupted=hidden=scriptedPaused=everPaused=everHidden=false;input=output=0;rigState='idle';pendingExpression=null;avatar.setPaused(false);avatar.setVisible(true);avatar.setLevel(0);avatar.setExpression(null);avatar.setState('idle');clock=0;nextSourceAt=0;lastSampleAt=-Infinity;sourceSampleCount=sourcePlaybackSamples=observedUpdateCount=0;samples=[];log=[];cues=[];sourceBeforePlayback=[];sourceInterruptionChecks=[];sourceEventCoverage=[];sourcePhraseIndices=new Set();sourceCueKinds=new Set();finalInquiryReached=false;expectedFinalPhraseIndex=-1;scenario=null;tape=[];next=0;
    }
    function start(value,{reducedMotion=false}={}){
      reset();scenario=value;tape=tapeFor(value);serial++;reduced=reducedMotion||value.reduced===true;startAt=now();clock=0;running=true;
      avatar.setReducedMotion?.(reduced);controller=expressionFactory.create({now:()=>clock,onExpression:applyExpression,onEvent:record});controller.setReducedMotion?.(reduced);update(0);return snapshot();
    }
    function handle(event){
      const id='fixture-turn-'+serial,responseId='fixture-response-'+serial;
      sourceEventCoverage.push({at:event.at,type:event.type});
      record({type:'fixture-'+event.type,kind:event.role||'',reason:event.value===undefined?'':String(event.value)});
      if(event.type==='begin'){controller.beginTurn({id,role:event.role,context:scenario.context||''});rigState=event.role==='user'?'listening':'thinking';}
      else if(event.type==='transcript'){
        controller.transcript({role:event.role,text:event.text,final:event.final,itemId:id,responseId,currentTurn:true});
        if(event.role==='assistant')expectedFinalPhraseIndex=controller.snapshot().phraseCount-1;
      }
      else if(event.type==='playback'){
        if(interrupted&&event.playing)return;playing=event.playing===true;controller.playback({playing,cleared:false,responseId});rigState=playing?'speaking':'listening';if(!playing)output=0;
      }
      else if(event.type==='listen'){listening=event.value;controller.setListening(listening);rigState='listening';}
      else if(event.type==='interrupt')interrupt(false);
      else if(event.type==='pause'){
        scriptedPaused=event.value===true;everPaused=everPaused||scriptedPaused;controller.setPaused(scriptedPaused);
        // Match production's animation-only pause: native source audio can
        // continue, while the rig and semantic planner remain canceled. Resume
        // admits current source RMS, never reconstructed old phrase cues.
        if(scriptedPaused)output=0;
      }
      else if(event.type==='hidden'){
        hidden=event.value===true;everHidden=everHidden||hidden;controller.setPaused(hidden);
        if(hidden){playing=false;output=0;rigState='listening';}
      }
      else if(event.type==='late'){controller.transcript({role:'assistant',text:scenario.text,final:true,itemId:id,responseId,currentTurn:false});controller.playback({playing:true,responseId});record({type:'fixture-late-old-response'});}
      else if(event.type==='finish')finish();
    }
    function levels(){
      const paused=scriptedPaused||hidden||manualPause;
      output=playing&&!interrupted&&!paused&&!scenario.analysisUnavailable&&!reduced?(Math.floor(clock/180)%7===0?0:[.12,.42,.25,.68,.18,.5][Math.floor(clock/100)%6]):0;
      input=listening&&!scenario.silence&&!paused?([0,.18,.43,.11,.3][Math.floor(clock/240)%5]):0;
      sourceSampleCount++;if(playing)sourcePlaybackSamples++;
      controller?.level({input,output});controller?.tick?.();
      const state=controller?.snapshot()||{},kind=state.cue?.kind||null;
      if(state.currentPhrase>=0)sourcePhraseIndices.add(state.currentPhrase);
      if(state.currentPhrase===expectedFinalPhraseIndex&&kind==='inquiry')finalInquiryReached=true;
      if(clock<900&&scenario.role!=='user')sourceBeforePlayback.push({at:clock,output,kind});
      if(interrupted)sourceInterruptionChecks.push({at:clock,output,state:rigState,kind});
    }
    function capture(force=false){
      // These are real observed rig snapshots, never the intermediate drained
      // source targets. Their sparse gaps remain visible in the final report.
      if(!force&&clock-lastSampleAt<100&&clock<scenario.duration)return null;lastSampleAt=clock;
      const face=faceSample(avatar),state=controller?.snapshot()||{};
      const entry={at:Math.round(clock),phase:clean(state.phase)||(!playing&&clock<900?'before-output':playing?'speaking':listening?'listening':'settling'),cueKind:state.cue?.kind||state.expression?.kind||null,currentPhrase:typeof state.currentPhrase==='number'?state.currentPhrase:null,input:round(input),output:round(output),face};
      samples.push(entry);if(samples.length>MAX_SAMPLES)samples.shift();onUpdate(snapshot());return entry;
    }
    function update(elapsed){
      if(!running||manualPause||!Number.isFinite(elapsed))return snapshot();
      const target=Math.max(clock,Math.min(scenario.duration,elapsed));let drains=0;observedUpdateCount++;
      while(running){
        const eventAt=next<tape.length?tape[next].at:Infinity,at=Math.min(nextSourceAt,eventAt);
        if(at>target)break;
        if(++drains>MAX_SOURCE_DRAINS)throw Error('Authored source drain exceeded its bounded tape.');
        clock=at;
        while(running&&next<tape.length&&tape[next].at===at)handle(tape[next++]);
        if(running)levels();
        if(nextSourceAt===at)nextSourceAt+=SOURCE_STEP_MS;
      }
      // Renderer time is an observation timestamp, not extra synthetic audio.
      clock=target;render();capture(!running);
      if(!running){const completed=result();onComplete(completed);onUpdate(snapshot());}
      return snapshot();
    }
    function step(){if(!running||manualPause)return snapshot();return update(now()-startAt);}
    function pause(value){
      if(!running)return;manualPause=value===true;everPaused=everPaused||manualPause;controller.setPaused(manualPause);
      if(manualPause){heldAt=now();output=0;}else startAt+=Math.max(0,now()-heldAt);
      render();onUpdate(snapshot());
    }
    function interrupt(paint=true){
      if(!running)return;interrupted=true;playing=false;listening=true;output=0;controller.cancel();controller.beginTurn({id:'fixture-interrupt-'+serial,role:'user',context:scenario.context||''});controller.setListening(true);rigState='listening';record({type:'fixture-interrupted'});if(paint)render();
    }
    function finish(){
      if(!running)return;playing=listening=false;input=output=0;controller.playback({playing:false,cleared:true,responseId:'fixture-response-'+serial});controller.cancel();scriptedPaused=hidden=false;rigState='listening';controller.tick?.();running=false;
    }
    function snapshot(){const state=controller?.snapshot()||{};return {scenario:scenario?.id||null,label:scenario?.label||null,running,manualPause,clockMs:Math.round(clock),durationMs:scenario?.duration||0,alignment:'authored-source-clock; production estimated playback cues; observed rig snapshots',nativeAudioTested:false,microphoneRequests:0,providerCalls:0,cartWrites:0,navigationChanges:0,inputRms:round(input),outputRms:round(output),interrupted,reducedFixture:reduced,sourceSampleCount,sourcePlaybackSamples,actualRenderedFrameSampleCount:samples.length,controller:{phase:state.phase||null,currentPhrase:state.currentPhrase??null,clockMs:round(state.clockMs),timing:state.timing||'estimated-audio-activity',cue:state.cue?{kind:state.cue.kind,intensity:round(state.cue.intensity)}:null},face:faceSample(avatar),cues:cues.slice(-20),log:log.slice(-MAX_LOG)};}
    function result(){
      const changed={},names=['brows','eyes','cheeks','smile'];for(const name of names)changed[name]=new Set(samples.map(value=>value.face.features[name]).filter(Boolean)).size;
      const channels=names.filter(name=>changed[name]>1),kinds=[...sourceCueKinds],observedKinds=[...new Set(samples.map(value=>value.cueKind).filter(Boolean))],mouthValues=[...new Set(samples.map(value=>value.face.mouthEnergy))],before=scenario?.role==='user'||sourceBeforePlayback.every(value=>value.output===0&&!value.kind)&&samples.filter(value=>value.at<900).every(value=>value.output===0&&!value.cueKind&&!value.face.expression),late=sourceInterruptionChecks.every(value=>value.output===0&&value.state!=='speaking')&&samples.filter(value=>value.at>=sourceInterruptionChecks[0]?.at).every(value=>value.face.mouthEnergy===0&&value.face.state!=='speaking');
      const normalSpeech=scenario?.role!=='user'&&!interrupted&&!everPaused&&!everHidden&&!reduced&&!scenario?.analysisUnavailable;
      const inquiryRequired=normalSpeech&&/\?\s*$/.test(scenario?.text||'');
      const invariants={beforePlaybackNoSpeechCue:before,noSyntheticPaidCalls:true,interruptionStopsMouth:sourceInterruptionChecks.length?late:null,listeningMouthClosed:scenario?.role==='user'?samples.every(value=>value.face.mouthEnergy===0):null,analysisUnavailableNoFakeMouth:scenario?.analysisUnavailable?samples.every(value=>value.face.mouthEnergy===0):null,reducedNoMovingMouth:reduced?samples.every(value=>value.face.mouthEnergy===0):null,finalInquiryCueReached:inquiryRequired?finalInquiryReached:null,finalOutputRmsZero:output===0};
      const maxGapMs=samples.reduce((gap,value,index)=>index?Math.max(gap,value.at-samples[index-1].at):gap,0),visibleIncomplete=maxGapMs>250;
      const failed=Object.entries(invariants).filter(([,value])=>value===false).map(([name])=>name),multi=!(scenario?.check==='multiple-cues')||kinds.length>1&&channels.length>=2;
      return {scenario:scenario?.id,label:scenario?.label,fixture:'explicit-synthetic-expression-events',result:failed.length||!multi?'needs-review':visibleIncomplete?'source-check-passed-visible-coverage-incomplete':'behavior-check-passed',failedInvariants:failed,multipleCueCheck:scenario?.check==='multiple-cues'?multi:null,distinctCueKinds:kinds,observedCueKinds:observedKinds,sourcePhraseIndices:[...sourcePhraseIndices],sourceEventCoverage,sourceSampleCount,sourcePlaybackSamples,sourceStepMs:SOURCE_STEP_MS,actualRenderedFrameSampleCount:samples.length,actualRenderedFrameSampleMeaning:'observed rig snapshots, at most one per browser update; not a GPU frame count',observedUpdateCount,maxObservedFrameGapMs:maxGapMs,visibleTrajectoryCoverage:visibleIncomplete?'incomplete-sparse-observations':'sampled-at-most-10Hz',changedFaceChannels:channels,distinctFeatureSamples:changed,distinctMouthEnergySamples:mouthValues.length,sampleCount:samples.length,invariants,rendererMode:avatar.snapshot().mode,providerCalls:0,microphoneRequests:0,gpuAppearance:'unverified',nativeSpokenTiming:'unverified',trustImprovement:'not-measured'};
    }
    return {start,update,step,pause,interrupt,reset,snapshot,result,destroy(){controller?.destroy();controller=null;running=false;}};
  }
  const api={SCENARIOS,tapeFor,faceSample,createRunner};
  if(typeof module==='object'&&module.exports)module.exports=api;
  else scope.BritesConciergeExpressionQA=api;
  if(!scope.document||scope.location?.pathname!=='/concierge-expression-qa.html')return;
  const doc=scope.document,select=doc.getElementById('scenario'),status=doc.getElementById('scenario-status'),text=doc.getElementById('current-text'),phase=doc.getElementById('phase-state'),pose=doc.getElementById('pose-metrics'),output=doc.getElementById('scenario-results'),transitions=doc.getElementById('transition-log'),renderer=doc.getElementById('renderer-state'),avatarMount=doc.getElementById('avatar');
  avatarMount.style.height='440px';select.style.maxWidth='100%';select.style.width='100%';pose.style.whiteSpace=output.style.whiteSpace=transitions.style.whiteSpace='pre-wrap';pose.style.overflowWrap=output.style.overflowWrap=transitions.style.overflowWrap='anywhere';transitions.style.maxHeight=output.style.maxHeight='420px';transitions.style.overflow=output.style.overflow='auto';
  for(const item of SCENARIOS){const option=doc.createElement('option');option.value=item.id;option.textContent=item.label;select.appendChild(option);}
  const results=[];let runner=null,avatar=null,reduced=false,suite=false,suiteIndex=0,frame=null,lastPaint=0;
  function show(snapshot){
    if(!snapshot)return;phase.textContent='Phase: '+(snapshot.controller.phase||snapshot.face.state)+' · '+(snapshot.clockMs/1000).toFixed(1)+' / '+(snapshot.durationMs/1000).toFixed(1)+' seconds · face '+(snapshot.controller.cue?.kind||snapshot.face.expression?.kind||'neutral');
    pose.textContent=JSON.stringify({fixture:snapshot.alignment,sourcePlaybackSamples:snapshot.sourcePlaybackSamples,actualRenderedFrameSampleCount:snapshot.actualRenderedFrameSampleCount,frameSampleMeaning:'Observed rig snapshots only; intermediate authored cues are not rendered evidence.',phase:snapshot.controller.phase,currentPhrase:snapshot.controller.currentPhrase,cue:snapshot.controller.cue,inputRms:snapshot.inputRms,outputRms:snapshot.outputRms,actualFace:snapshot.face,providerCalls:0,microphoneRequests:0},null,2);
    transitions.textContent=JSON.stringify(snapshot.log,null,2);const mode=snapshot.face.mode;renderer.textContent=mode==='webgl'?'WebGL scene is active. GPU appearance and shadows still require visual review.':mode==='fallback'?'Actual animated 2-D fallback is active. GPU, shadows and FPS remain unverified.':'The production renderer is preparing.';
  }
  function complete(value){results.push(value);if(results.length>MAX_RESULTS)results.shift();output.textContent=JSON.stringify({fixture:'synthetic-events-actual-face-rig',completed:results.length,results},null,2);status.textContent=value.label+': '+value.result.replaceAll('-',' ')+'. Native spoken timing and GPU quality remain unverified.';if(suite){suiteIndex++;if(suiteIndex<SCENARIOS.length){start(SCENARIOS[suiteIndex]);}else{suite=false;status.textContent='All '+SCENARIOS.length+' authored scenarios completed. Inspect results and visual transitions; hardware acceptance remains separate.';}}}
  function start(item){if(!runner)return;select.value=item.id;text.textContent=item.role==='user'?'Synthetic shopper: '+(item.text||'Speech activity before any words are available.'):('Synthetic assistant: '+item.text);runner.start(item,{reducedMotion:reduced});status.textContent='Running '+item.label+'. Synthetic events; no microphone or OpenAI call.';doc.getElementById('pause-sequence').setAttribute('aria-pressed','false');doc.getElementById('pause-sequence').textContent='Pause sequence';}
  function loop(at){if(runner?.snapshot().running){runner.step();if(at-lastPaint>=100){show(runner.snapshot());lastPaint=at;}}frame=scope.requestAnimationFrame(loop);}
  try{
    avatar=scope.BritesConciergeAvatar.create({container:avatarMount,assetBase:scope.location.origin+'/',visible:true,greetingOnOpen:false});
    runner=createRunner({expressionFactory:scope.BritesConciergeExpression,avatar,now:()=>scope.performance.now(),onUpdate:show,onComplete:complete});
    avatar.ready.then(()=>{status.textContent='Production rig ready. Choose an authored scenario.';show(runner.snapshot());});frame=scope.requestAnimationFrame(loop);
  }catch{status.textContent='The production planner or rig is unavailable. This fixture cannot simulate a pass.';doc.querySelectorAll('button').forEach(button=>button.disabled=true);}
  doc.getElementById('run-scenario').onclick=()=>{suite=false;start(SCENARIOS.find(value=>value.id===select.value)||SCENARIOS[0]);};
  doc.getElementById('run-suite').onclick=()=>{suite=true;suiteIndex=0;results.length=0;start(SCENARIOS[0]);};
  doc.getElementById('pause-sequence').onclick=function(){const paused=this.getAttribute('aria-pressed')!=='true';runner?.pause(paused);this.setAttribute('aria-pressed',String(paused));this.textContent=paused?'Continue sequence':'Pause sequence';status.textContent=paused?'Sequence and face animation paused. Old cues are canceled.':'Sequence resumed; canceled cues will not replay.';};
  doc.getElementById('interrupt-sequence').onclick=()=>{runner?.interrupt();show(runner?.snapshot());status.textContent='Synthetic shopper interrupted. Pending speaking cues were canceled.';};
  doc.getElementById('reset-sequence').onclick=()=>{suite=false;runner?.reset();show(runner?.snapshot());status.textContent='Sequence reset. No previous cues will replay.';};
  doc.getElementById('reduce-motion').onclick=function(){reduced=this.getAttribute('aria-pressed')!=='true';this.setAttribute('aria-pressed',String(reduced));suite=false;runner?.reset();avatar?.setReducedMotion?.(reduced);doc.getElementById('pause-sequence').setAttribute('aria-pressed','false');doc.getElementById('pause-sequence').textContent='Pause sequence';show(runner?.snapshot());status.textContent='Reduced-motion fixture '+(reduced?'enabled':'disabled')+'. Run a scenario to apply this setting. This does not change the browser configuration.';};
  doc.addEventListener('visibilitychange',()=>{if(doc.hidden){suite=false;runner?.pause(true);status.textContent='The real browser page was hidden. Sequence paused and old cues canceled.';}});
  scope.addEventListener('pagehide',()=>{if(frame!==null)scope.cancelAnimationFrame(frame);runner?.destroy();avatar?.destroy();},{once:true});
})(typeof window!=='undefined'?window:globalThis);
