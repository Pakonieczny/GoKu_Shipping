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
    {id:'generation',label:'Spectrum and selection while audio continues',text:'The finish changes the look of a piece. A small symbol may be a personal reminder. Which style feels right?',duration:14000,check:'multiple-cues',generationAhead:true,focusAt:3500,selectionAt:5000,clearFocusAt:7800,quietAt:6200,quietUntil:6600},
    {id:'late',label:'Late old-response events after a new turn',text:'That sounds like a joyful milestone. We can find a personal design. Does she wear silver or gold?',duration:10500,check:'interrupted',interruptAt:4100,lateAt:5700}
  ].map(item=>Object.freeze(item)));
  const finite=value=>Number.isFinite(value)?value:null;
  const round=value=>Number.isFinite(value)?Math.round(value*1000)/1000:null;
  const clean=value=>typeof value==='string'?value.slice(0,400):'';
  const SPECTRA=Object.freeze([
    Object.freeze([.9,.72,.4,.18,.07,.03]),
    Object.freeze([.13,.36,.78,.66,.25,.08]),
    Object.freeze([.03,.08,.19,.37,.72,.92])
  ]);
  const INSPECTION_PHASES=Object.freeze([
    {id:'before-output',label:'Before output',at:800},
    {id:'speaking',label:'Speaking · middle spectrum',at:1200},
    {id:'spectrum-high',label:'Speaking · higher spectrum',at:1800},
    {id:'quiet',label:'Quiet output sample',at:1300},
    {id:'product-focus',label:'Product focus while speaking',scenarioId:'generation',at:3600},
    {id:'product-selection',label:'Product selection while speaking',scenarioId:'generation',at:5000},
    {id:'paused',label:'During animation-only pause',scenarioId:'pause',at:4100},
    {id:'pause-resumed',label:'After animation-only pause',scenarioId:'pause',at:7000},
    {id:'hidden',label:'During hidden guide',scenarioId:'hidden',at:4100},
    {id:'shown',label:'After guide returns',scenarioId:'hidden',at:7200},
    {id:'listening',label:'Listening before words',scenarioId:'listen',at:1200},
    {id:'listening-input',label:'Listening · held synthetic incoming energy',scenarioId:'listen',at:1800},
    {id:'listening-support',label:'Listening after supportive words',scenarioId:'listen-support',at:4000},
    {id:'inquiry',label:'Final spoken question'}
  ].map(value=>Object.freeze(value)));
  function authoredSignal(at,amplitude){
    const level=Number.isFinite(amplitude)?Math.max(0,Math.min(1,amplitude)):0;
    if(!Number.isFinite(at)||level<=.015)return {amplitude:0,bands:[0,0,0,0,0,0],brightness:0,valid:false};
    const spectrum=SPECTRA[Math.floor(Math.max(0,at)/700)%SPECTRA.length],bands=spectrum.map(value=>round(value*level)),total=bands.reduce((a,b)=>a+b,0);
    return {amplitude:round(level),bands,brightness:round(total?bands.reduce((sum,value,index)=>sum+value*(index/5),0)/total:0),valid:true};
  }
  function inspectionTarget(scenario,phaseId,expressionFactory){
    const phase=INSPECTION_PHASES.find(value=>value.id===phaseId);
    if(!phase||!SCENARIOS.includes(scenario))return null;
    if(phase.scenarioId&&phase.scenarioId!==scenario.id)return null;
    if(phase.id==='inquiry'){
      if(scenario.role==='user'||scenario.pauseAt||scenario.hideAt||scenario.interruptAt||scenario.reduced||scenario.analysisUnavailable)return null;
      const question=expressionFactory?.plan?.(scenario.text,{context:scenario.context||''})?.findLast(value=>value.kind==='inquiry');
      return question?Math.min(scenario.duration-750,900+question.startMs+Math.min(500,question.durationMs*.3)):null;
    }
    return Math.min(scenario.duration-50,phase.at);
  }
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
    if(scenario.focusAt)events.push({at:scenario.focusAt,type:'focus',choice:'a'});
    if(scenario.selectionAt)events.push({at:scenario.selectionAt,type:'selection',choice:'b'});
    if(scenario.clearFocusAt)events.push({at:scenario.clearFocusAt,type:'focus',choice:null});
    if(scenario.quietAt)events.push({at:scenario.quietAt,type:'quiet',value:true},{at:scenario.quietUntil,type:'quiet',value:false});
    events.push({at:scenario.duration,type:'finish'});
    return events.sort((a,b)=>a.at-b.at);
  }
  function faceSample(avatar){
    const state=avatar.snapshot(),frame=avatar.element,raw=state.facePose||state.pose||state.face||{};
    const attr=(name,key)=>frame?.querySelector('.brites-avatar__'+name)?.getAttribute(key)||'';
    const css=name=>frame?.style?.getPropertyValue(name)||'';
    const features={brows:[raw.faceBrowLift,raw.faceBrowTilt].filter(Number.isFinite).map(round).join(',')||attr('brow--left','d'),eyes:[raw.eyeScaleX,raw.eyeScaleY,raw.eyeSmile,raw.eyeOpen].filter(Number.isFinite).map(round).join(',')||attr('ribbon--left','d'),cheeks:Number.isFinite(raw.cheekGlow)?String(round(raw.cheekGlow)):attr('cheek--left','opacity'),smile:Number.isFinite(raw.smileCurve)?String(round(raw.smileCurve)):attr('smile-signal','d')};
    const energy=finite(raw.speechEnergy)??finite(Number(css('--brites-speech-level')))??0,signal=state.speechSignal,visual=state.speechVisual,incoming=state.inputSignal;
    const paths=selector=>[...(frame?.querySelectorAll(selector)||[])].map(node=>({d:node.getAttribute('d')||'',opacity:node.getAttribute('opacity')||'',stroke:node.getAttribute('stroke')||''}));
    return {mode:state.mode,state:state.state,paused:state.paused===true,visible:state.visible===true,reducedMotion:state.reducedMotion===true,expression:state.expression?{kind:state.expression.kind,intensity:round(state.expression.intensity)}:null,emotion:state.emotion||null,features,mouthEnergy:round(energy),speechEnergy:round(energy),inputEnergy:round(raw.inputEnergy)||0,inputSignal:incoming?{amplitude:round(incoming.amplitude),bands:Array.isArray(incoming.bands)?incoming.bands.map(round):null,brightness:round(incoming.brightness),valid:incoming.valid===true}:null,curveClosed:visual?.curveClosed===true,openingEllipsePresent:!!frame?.querySelector('ellipse.brites-avatar__speech-mouth'),curvePath:attr('smile-signal','d'),curveTag:frame?.querySelector('.brites-avatar__smile-signal')?.localName||null,speechSignal:signal?{amplitude:round(signal.amplitude),bands:Array.isArray(signal.bands)?signal.bands.map(round):null,brightness:round(signal.brightness),valid:signal.valid===true}:null,speechVisual:visual?{curveClosed:visual.curveClosed===true,rippleActive:visual.rippleActive===true,amplitude:round(visual.amplitude),bands:Array.isArray(visual.bands)?visual.bands.map(round):null,brightness:round(visual.brightness)}:null,ripplePaths:paths('.brites-avatar__speech-ripple'),bandPaths:paths('.brites-avatar__speech-band'),productFocus:state.productFocus?{x:round(state.productFocus.x),y:round(state.productFocus.y)}:null,facePose:Object.fromEntries(Object.entries(raw).filter(([key,value])=>/^(?:faceBrow|eye|cheekGlow|smileCurve|head|speechEnergy|inputEnergy|mouthOpen|mouthCurve)/.test(key)&&Number.isFinite(value)).map(([key,value])=>[key,round(value)]))};
  }
  function safeEvent(value){
    if(typeof value==='string')return {type:clean(value)};
    if(!value||typeof value!=='object')return {type:'expression-event'};
    const result={};for(const key of ['type','kind','intensity','index','phase','reason','timing','responseId','itemId','currentPhrase','cueIndex','queueLength','accepted','rejected'])if(['string','number','boolean'].includes(typeof value[key]))result[key]=typeof value[key]==='string'?clean(value[key]):value[key];
    if(value.cue?.kind)result.kind=clean(value.cue.kind);return result;
  }
  function createRunner({expressionFactory,avatar,onUpdate=()=>{},onComplete=()=>{},now=()=>0,setTimer=(callback,delay)=>scope.setTimeout(callback,delay),clearTimer=id=>scope.clearTimeout(id)}={}){
    if(!expressionFactory?.create||!avatar?.setExpression)throw Error('The production expression planner and face rig are required.');
    // The authored source clock is separate from renderer updates. A sparse
    // browser update drains at most 50ms of source audio per planner tick, but
    // only its final pose is sent to the real rig and captured once.
    const SOURCE_STEP_MS=50,MAX_SOURCE_DRAINS=400;
    let clock=0,serial=0,controller=null,scenario=null,tape=[],next=0,nextSourceAt=0,running=false,manualPause=false,inspectionHeld=false,inspectionUsed=false,heldPhase=null,startAt=0,heldAt=0,playing=false,listening=false,interrupted=false,hidden=false,reduced=false,quiet=false,scriptedPaused=false,everPaused=false,everHidden=false,rigState='idle',pendingExpression=null,output=0,input=0,outputSignal=authoredSignal(0,0),inputSignal=authoredSignal(0,0),heldInputTimer=null,heldInputApplications=0,sourceFocusChoice=null,selectedChoice=null,appliedFocusChoice=null,lastSampleAt=-Infinity,sourceSampleCount=0,sourcePlaybackSamples=0,observedUpdateCount=0,sourceBeforePlayback=[],sourceInterruptionChecks=[],sourceEventCoverage=[],sourcePhraseIndices=new Set(),sourceCueKinds=new Set(),finalInquiryReached=false,expectedFinalPhraseIndex=-1,samples=[],log=[],cues=[];
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
    function stopHeldInput(){if(heldInputTimer!==null)clearTimer(heldInputTimer);heldInputTimer=null;}
    function heldInputActive(){return running&&inspectionHeld&&['listening-input','listening-support'].includes(heldPhase)&&listening&&rigState==='listening'&&inputSignal.valid&&!manualPause&&!scriptedPaused&&!hidden&&!reduced;}
    function armHeldInput(){
      stopHeldInput();if(!heldInputActive())return;
      // Explicit visual inspection reapplies the SAME labeled synthetic sample.
      // It advances neither source time, source packets, controller phrases nor
      // recorded frame observations. Native source expiry stays unchanged.
      heldInputTimer=setTimer(()=>{heldInputTimer=null;if(!heldInputActive())return;avatar.setInputSignal?.(inputSignal);heldInputApplications++;armHeldInput();},100);
    }
    function render(){
      avatar.setPaused(manualPause||scriptedPaused);avatar.setVisible(!hidden);avatar.setState(rigState);avatar.setExpression(pendingExpression);
      if(avatar.setSpeechSignal)avatar.setSpeechSignal(outputSignal);else avatar.setLevel(output);
      avatar.setInputSignal?.(rigState==='listening'?inputSignal:authoredSignal(clock,0));
      applyProductFocus();
    }
    function applyProductFocus(){
      if(hidden||scriptedPaused||manualPause||sourceFocusChoice===appliedFocusChoice)return;
      if(sourceFocusChoice)avatar.focusProduct?.({x:sourceFocusChoice==='a'?-.35:.42,y:.1});else avatar.clearFocus?.();
      appliedFocusChoice=sourceFocusChoice;
    }
    function reset(){
      stopHeldInput();controller?.destroy();controller=null;running=false;manualPause=inspectionHeld=inspectionUsed=false;heldPhase=null;playing=listening=interrupted=hidden=quiet=scriptedPaused=everPaused=everHidden=false;input=output=0;outputSignal=inputSignal=authoredSignal(0,0);heldInputApplications=0;avatar.setInputSignal?.(inputSignal);sourceFocusChoice=selectedChoice=appliedFocusChoice=null;rigState='idle';pendingExpression=null;avatar.setPaused(false);avatar.setVisible(true);if(avatar.setSpeechSignal)avatar.setSpeechSignal(outputSignal);else avatar.setLevel(0);avatar.clearFocus?.();avatar.setExpression(null);avatar.setState('idle');clock=0;nextSourceAt=0;lastSampleAt=-Infinity;sourceSampleCount=sourcePlaybackSamples=observedUpdateCount=0;samples=[];log=[];cues=[];sourceBeforePlayback=[];sourceInterruptionChecks=[];sourceEventCoverage=[];sourcePhraseIndices=new Set();sourceCueKinds=new Set();finalInquiryReached=false;expectedFinalPhraseIndex=-1;scenario=null;tape=[];next=0;
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
      else if(event.type==='focus')sourceFocusChoice=event.choice;
      else if(event.type==='selection'){sourceFocusChoice=selectedChoice=event.choice;}
      else if(event.type==='quiet')quiet=event.value===true;
      else if(event.type==='finish')finish();
    }
    function levels(){
      const paused=scriptedPaused||hidden||manualPause;
      output=playing&&!interrupted&&!paused&&!quiet&&!scenario.analysisUnavailable&&!reduced?(Math.floor(clock/180)%7===0?0:[.12,.42,.25,.68,.18,.5][Math.floor(clock/100)%6]):0;
      outputSignal=authoredSignal(clock,output);
      input=listening&&!scenario.silence&&!paused&&!reduced&&!scenario.analysisUnavailable?([0,.18,.43,.11,.3][Math.floor(clock/240)%5]):0;
      inputSignal=authoredSignal(clock,input);
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
      const entry={at:Math.round(clock),observedAtMs:round(now()),inspectionHeld,phase:clean(state.phase)||(!playing&&clock<900?'before-output':playing?'speaking':listening?'listening':'settling'),cueKind:state.cue?.kind||state.expression?.kind||null,currentPhrase:typeof state.currentPhrase==='number'?state.currentPhrase:null,input:round(input),output:round(output),outputSignal:{...outputSignal,bands:[...outputSignal.bands]},inputSignal:{...inputSignal,bands:[...inputSignal.bands]},selectedChoice,focusChoice:sourceFocusChoice,face};
      samples.push(entry);if(samples.length>MAX_SAMPLES)samples.shift();onUpdate(snapshot());return entry;
    }
    function update(elapsed){
      if(!running||manualPause||inspectionHeld||!Number.isFinite(elapsed))return snapshot();
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
    function step(){if(!running||manualPause||inspectionHeld)return snapshot();return update(now()-startAt);}
    function inspectAt(elapsed,phaseId='authored-timestamp'){
      if(!running||manualPause||!Number.isFinite(elapsed)||elapsed<clock||elapsed>=scenario.duration)return false;
      inspectionUsed=true;inspectionHeld=false;update(elapsed);inspectionHeld=true;heldPhase=clean(phaseId);heldAt=now();armHeldInput();onUpdate(snapshot());return snapshot();
    }
    function continueInspection(){if(!running||!inspectionHeld)return false;stopHeldInput();avatar.setInputSignal?.(authoredSignal(clock,0));inspectionHeld=false;heldPhase=null;startAt=now()-clock;onUpdate(snapshot());return snapshot();}
    function observe(){if(!scenario)return false;capture(true);return snapshot();}
    function productAction(type,choice){
      if(!running||!['focus','selection'].includes(type)||![null,'a','b'].includes(choice))return false;
      sourceFocusChoice=choice;if(type==='selection')selectedChoice=choice;
      // This explicit author UI action does not change role, clock, playback,
      // expression or the current measured-shape signal.
      applyProductFocus();record({type:'author-product-'+type,reason:choice||'cleared'});capture(true);return snapshot();
    }
    function pause(value){
      if(!running)return;manualPause=value===true;everPaused=everPaused||manualPause;controller.setPaused(manualPause);
      if(manualPause){stopHeldInput();heldAt=now();input=output=0;inputSignal=outputSignal=authoredSignal(clock,0);}else startAt+=Math.max(0,now()-heldAt);
      render();onUpdate(snapshot());
    }
    function interrupt(paint=true){
      if(!running)return;stopHeldInput();interrupted=true;playing=false;listening=true;input=output=0;inputSignal=outputSignal=authoredSignal(clock,0);controller.cancel();controller.beginTurn({id:'fixture-interrupt-'+serial,role:'user',context:scenario.context||''});controller.setListening(true);rigState='listening';record({type:'fixture-interrupted'});if(paint)render();
    }
    function finish(){
      if(!running)return;stopHeldInput();playing=listening=false;input=output=0;inputSignal=outputSignal=authoredSignal(clock,0);controller.playback({playing:false,cleared:true,responseId:'fixture-response-'+serial});controller.cancel();scriptedPaused=hidden=false;rigState='listening';controller.tick?.();running=false;
    }
    function snapshot(){const state=controller?.snapshot()||{};return {scenario:scenario?.id||null,label:scenario?.label||null,running,manualPause,inspectionHeld,inspectionUsed,heldPhase,clockMs:Math.round(clock),durationMs:scenario?.duration||0,alignment:'authored-source-clock; synthetic amplitude/spectrum; production rig snapshots',nativeAudioTested:false,microphoneRequests:0,providerCalls:0,cartWrites:0,navigationChanges:0,inputRms:round(input),outputRms:round(output),authoredOutputSignal:{...outputSignal,bands:[...outputSignal.bands]},authoredInputSignal:{...inputSignal,bands:[...inputSignal.bands]},heldSyntheticInput:{active:heldInputActive(),applicationsAfterSourceSample:heldInputApplications,meaning:'Same explicitly held synthetic incoming sample reapplied to paint the current rig; no additional source packets, phrase time or recorded frame observations.'},product:{focusChoice:sourceFocusChoice,selectedChoice},interrupted,reducedFixture:reduced,sourceSampleCount,sourcePlaybackSamples,actualRenderedFrameSampleCount:samples.length,controller:{phase:state.phase||null,currentPhrase:state.currentPhrase??null,clockMs:round(state.clockMs),timing:state.timing||'estimated-audio-activity',cue:state.cue?{kind:state.cue.kind,intensity:round(state.cue.intensity)}:null},face:faceSample(avatar),cues:cues.slice(-20),log:log.slice(-MAX_LOG)};}
    function result(){
      const changed={},names=['brows','eyes','cheeks','smile'];for(const name of names)changed[name]=new Set(samples.map(value=>value.face.features[name]).filter(Boolean)).size;
      const channels=names.filter(name=>changed[name]>1),kinds=[...sourceCueKinds],observedKinds=[...new Set(samples.map(value=>value.cueKind).filter(Boolean))],mouthValues=[...new Set(samples.map(value=>value.face.mouthEnergy))],before=scenario?.role==='user'||sourceBeforePlayback.every(value=>value.output===0&&!value.kind)&&samples.filter(value=>value.at<900).every(value=>value.output===0&&!value.cueKind&&!value.face.expression),late=sourceInterruptionChecks.every(value=>value.output===0&&value.state!=='speaking')&&samples.filter(value=>value.at>=sourceInterruptionChecks[0]?.at).every(value=>value.face.mouthEnergy===0&&value.face.state!=='speaking');
      const normalSpeech=scenario?.role!=='user'&&!interrupted&&!everPaused&&!everHidden&&!reduced&&!scenario?.analysisUnavailable;
      const inquiryRequired=normalSpeech&&/\?\s*$/.test(scenario?.text||'');
      const selectionSamples=samples.filter(value=>value.selectedChoice&&value.output>.015),quietSamples=samples.filter(value=>value.output<=.015);
      const signalMatches=value=>value.face.speechSignal?.valid===true&&Math.abs(value.face.speechSignal.amplitude-value.outputSignal.amplitude)<.001&&Math.abs(value.face.speechSignal.brightness-value.outputSignal.brightness)<.001&&JSON.stringify(value.face.speechSignal.bands)===JSON.stringify(value.outputSignal.bands);
      // Signal retention is distinct from the rig's measured attack envelope:
      // the first valid source sample can precede a visible ripple. Report that
      // actual appearance below rather than inventing a positive first frame.
      const invariants={beforePlaybackNoSpeechCue:before,noSyntheticPaidCalls:true,closedCurveWithoutOpeningEllipse:samples.every(value=>value.face.curveClosed===true&&!value.face.openingEllipsePresent),quietSignalClearsRipple:quietSamples.every(value=>value.face.speechVisual?.rippleActive===false&&value.face.speechSignal?.valid!==true),productSelectionKeepsVoiceSignal:scenario?.selectionAt&&selectionSamples.length?selectionSamples.every(value=>value.face.state==='speaking'&&signalMatches(value)):null,interruptionStopsMouth:sourceInterruptionChecks.length?late:null,listeningMouthClosed:scenario?.role==='user'?samples.every(value=>value.face.mouthEnergy===0):null,analysisUnavailableNoFakeMouth:scenario?.analysisUnavailable?samples.every(value=>value.face.mouthEnergy===0):null,reducedNoMovingMouth:reduced?samples.every(value=>value.face.mouthEnergy===0):null,finalInquiryCueReached:inquiryRequired?finalInquiryReached:null,finalOutputRmsZero:output===0};
      const maxGapMs=samples.reduce((gap,value,index)=>index?Math.max(gap,value.at-samples[index-1].at):gap,0),visibleIncomplete=maxGapMs>250;
      const failed=Object.entries(invariants).filter(([,value])=>value===false).map(([name])=>name),multi=!(scenario?.check==='multiple-cues')||kinds.length>1&&channels.length>=2;
      const observedFaceKinds=[...new Set(samples.map(value=>value.face.expression?.kind).filter(Boolean))],spectra=new Set(samples.filter(value=>value.face.speechSignal?.valid).map(value=>JSON.stringify(value.face.speechSignal.bands))),displayedSpectra=new Set(samples.filter(value=>value.face.speechVisual?.rippleActive).map(value=>JSON.stringify(value.face.speechVisual.bands))),ripplePaths=new Set(samples.map(value=>JSON.stringify(value.face.ripplePaths)+JSON.stringify(value.face.bandPaths))),observationGap=samples.reduce((gap,value,index)=>index?Math.max(gap,value.observedAtMs-samples[index-1].observedAtMs):gap,0);
      return {scenario:scenario?.id,label:scenario?.label,fixture:'explicit-synthetic-expression-and-spectrum-events',result:failed.length||!multi?'needs-review':visibleIncomplete?'source-check-passed-visible-coverage-incomplete':'behavior-check-passed',failedInvariants:failed,multipleCueCheck:scenario?.check==='multiple-cues'?multi:null,distinctCueKinds:kinds,observedCueKinds:observedKinds,observedControllerCueKinds:observedKinds,observedActualFaceExpressionKinds:observedFaceKinds,rigExpressionMeaning:'expression targets on the actual rig; eased displayed geometry is reported in changedFaceChannels and facePose',distinctObservedSpectra:spectra.size,distinctObservedSignalTargets:spectra.size,distinctObservedDisplayedSpectra:displayedSpectra.size,distinctObservedRipplePaths:ripplePaths.size,selectionSourcePositiveSamples:selectionSamples.length,selectionObservedRippleSamples:selectionSamples.filter(value=>value.face.speechVisual?.rippleActive===true).length,sourcePhraseIndices:[...sourcePhraseIndices],sourceEventCoverage,sourceSampleCount,sourcePlaybackSamples,sourceStepMs:SOURCE_STEP_MS,actualRenderedFrameSampleCount:samples.length,actualRenderedFrameSampleMeaning:'real rig snapshots, once per source update or explicit observe/product action; not a GPU frame count',observedUpdateCount,maxObservedFrameGapMs:maxGapMs,maxObservedSourceTimeGapMs:maxGapMs,maxObservationTimeGapMs:observationGap,visibleTrajectoryCoverage:inspectionUsed?'authored-phase-inspection-no-natural-cadence-claim':visibleIncomplete?'incomplete-sparse-observations':'sampled-at-most-10Hz',changedFaceChannels:channels,distinctFeatureSamples:changed,distinctMouthEnergySamples:mouthValues.length,sampleCount:samples.length,invariants,rendererMode:avatar.snapshot().mode,providerCalls:0,microphoneRequests:0,gpuAppearance:'unverified',nativeSpokenTiming:'unverified',trustImprovement:'not-measured'};
    }
    return {start,update,step,pause,inspectAt,continueInspection,observe,productAction,interrupt,reset,snapshot,result,destroy(){stopHeldInput();input=output=0;inputSignal=outputSignal=authoredSignal(clock,0);avatar.setInputSignal?.(inputSignal);if(avatar.setSpeechSignal)avatar.setSpeechSignal(outputSignal);else avatar.setLevel(0);controller?.destroy();controller=null;running=playing=listening=false;}};
  }
  const api={SCENARIOS,INSPECTION_PHASES,authoredSignal,inspectionTarget,tapeFor,faceSample,createRunner};
  if(typeof module==='object'&&module.exports)module.exports=api;
  else scope.BritesConciergeExpressionQA=api;
  if(!scope.document||scope.location?.pathname!=='/concierge-expression-qa.html')return;
  const doc=scope.document,select=doc.getElementById('scenario'),inspection=doc.getElementById('inspection-phase'),status=doc.getElementById('scenario-status'),text=doc.getElementById('current-text'),phase=doc.getElementById('phase-state'),pose=doc.getElementById('pose-metrics'),output=doc.getElementById('scenario-results'),transitions=doc.getElementById('transition-log'),renderer=doc.getElementById('renderer-state'),avatarMount=doc.getElementById('avatar');
  avatarMount.style.height='440px';select.style.maxWidth='100%';select.style.width='100%';pose.style.whiteSpace=output.style.whiteSpace=transitions.style.whiteSpace='pre-wrap';pose.style.overflowWrap=output.style.overflowWrap=transitions.style.overflowWrap='anywhere';transitions.style.maxHeight=output.style.maxHeight='420px';transitions.style.overflow=output.style.overflow='auto';
  doc.querySelector('h1').style.fontSize='clamp(28px,3.5vw,42px)';inspection.style.maxWidth='100%';inspection.style.width='100%';
  for(const item of SCENARIOS){const option=doc.createElement('option');option.value=item.id;option.textContent=item.label;select.appendChild(option);}
  for(const item of INSPECTION_PHASES){const option=doc.createElement('option');option.value=item.id;option.textContent=item.label;inspection.appendChild(option);}
  const results=[];let runner=null,avatar=null,reduced=false,suite=false,suiteIndex=0,frame=null,lastPaint=0;
  function show(snapshot){
    if(!snapshot)return;phase.textContent='Phase: '+(snapshot.controller.phase||snapshot.face.state)+' · '+(snapshot.clockMs/1000).toFixed(1)+' / '+(snapshot.durationMs/1000).toFixed(1)+' seconds · controller '+(snapshot.controller.cue?.kind||'neutral')+' · rig target '+(snapshot.face.expression?.kind||'neutral')+(snapshot.inspectionHeld?' · HELD authored sample':'');
    pose.textContent=JSON.stringify({fixture:snapshot.alignment,inspectionHeld:snapshot.inspectionHeld,inspectionUsed:snapshot.inspectionUsed,heldPhase:snapshot.heldPhase,sourceClockMs:snapshot.clockMs,sourcePlaybackSamples:snapshot.sourcePlaybackSamples,actualRenderedFrameSampleCount:snapshot.actualRenderedFrameSampleCount,frameSampleMeaning:'Observed rig snapshots only; intermediate authored cues are not rendered evidence.',phase:snapshot.controller.phase,currentPhrase:snapshot.controller.currentPhrase,controllerCue:snapshot.controller.cue,authoredOutputSignal:snapshot.authoredOutputSignal,authoredInputSignal:snapshot.authoredInputSignal,heldSyntheticInput:snapshot.heldSyntheticInput,inputRms:snapshot.inputRms,outputRms:snapshot.outputRms,product:snapshot.product,actualFace:snapshot.face,providerCalls:0,microphoneRequests:0},null,2);
    doc.getElementById('continue-inspection').disabled=!snapshot.inspectionHeld;doc.getElementById('pause-sequence').disabled=snapshot.inspectionHeld;
    doc.getElementById('fixture-selection').textContent=snapshot.product.selectedChoice?'Synthetic selection '+snapshot.product.selectedChoice.toUpperCase()+'.':snapshot.product.focusChoice?'Synthetic focus '+snapshot.product.focusChoice.toUpperCase()+'.':'No synthetic selection.';
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
  doc.getElementById('inspect-phase').onclick=()=>{
    suite=false;const selected=INSPECTION_PHASES.find(value=>value.id===inspection.value),item=selected?.scenarioId?SCENARIOS.find(value=>value.id===selected.scenarioId):SCENARIOS.find(value=>value.id===select.value);
    const at=inspectionTarget(item,selected?.id,scope.BritesConciergeExpression);
    if(at===null){status.textContent='That phase does not apply to this scenario. Choose a complete speaking scenario for the question inspector.';return;}
    start(item);runner.inspectAt(at,selected.id);show(runner.snapshot());status.textContent='Held '+selected.label+' at '+(at/1000).toFixed(2)+' source seconds. Authored replay; no live audio. Intermediate source poses were not rendered.';
  };
  doc.getElementById('continue-inspection').onclick=()=>{if(runner?.continueInspection())status.textContent='Authored source continues from the held timestamp. This is separate from canceling Pause sequence.';};
  doc.getElementById('observe-inspection').onclick=()=>{if(runner?.observe())status.textContent='Current actual rig pose captured once. Source time and events were not advanced.';};
  doc.getElementById('focus-sample').onclick=()=>{if(runner?.productAction('focus','a'))status.textContent='Synthetic product focus changed; current playback role and speech signal were preserved.';};
  doc.getElementById('select-sample').onclick=()=>{if(runner?.productAction('selection','b'))status.textContent='Synthetic selection changed; current playback role and speech signal were preserved.';};
  doc.getElementById('clear-sample').onclick=()=>{if(runner?.productAction('focus',null))status.textContent='Synthetic focus cleared without changing the current speech signal.';};
  doc.getElementById('pause-sequence').onclick=function(){const paused=this.getAttribute('aria-pressed')!=='true';runner?.pause(paused);this.setAttribute('aria-pressed',String(paused));this.textContent=paused?'Continue sequence':'Pause sequence';status.textContent=paused?'Sequence and face animation paused. Old cues are canceled.':'Sequence resumed; canceled cues will not replay.';};
  doc.getElementById('interrupt-sequence').onclick=()=>{runner?.interrupt();show(runner?.snapshot());status.textContent='Synthetic shopper interrupted. Pending speaking cues were canceled.';};
  doc.getElementById('reset-sequence').onclick=()=>{suite=false;runner?.reset();show(runner?.snapshot());status.textContent='Sequence reset. No previous cues will replay.';};
  doc.getElementById('reduce-motion').onclick=function(){reduced=this.getAttribute('aria-pressed')!=='true';this.setAttribute('aria-pressed',String(reduced));suite=false;runner?.reset();avatar?.setReducedMotion?.(reduced);doc.getElementById('pause-sequence').setAttribute('aria-pressed','false');doc.getElementById('pause-sequence').textContent='Pause sequence';show(runner?.snapshot());status.textContent='Reduced-motion fixture '+(reduced?'enabled':'disabled')+'. Run a scenario to apply this setting. This does not change the browser configuration.';};
  doc.addEventListener('visibilitychange',()=>{if(doc.hidden){suite=false;runner?.pause(true);status.textContent='The real browser page was hidden. Sequence paused and old cues canceled.';}});
  scope.addEventListener('pagehide',()=>{if(frame!==null)scope.cancelAnimationFrame(frame);runner?.destroy();avatar?.destroy();},{once:true});
})(typeof window!=='undefined'?window:globalThis);
