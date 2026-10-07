(function(){
  'use strict';
  const start=document.querySelector('#start'),stop=document.querySelector('#stop'),status=document.querySelector('#status'),transcript=document.querySelector('#transcript'),level=document.querySelector('#level'),audio=document.querySelector('#audio'),evidence=document.querySelector('#evidence'),signalEvidence=document.querySelector('#signal-evidence'),renderer=document.querySelector('#renderer'),focusOption=document.querySelector('#focus-during-speech'),fixtureTray=document.querySelector('#fixture-tray');
  const zero=()=>({amplitude:0,bands:[0,0,0,0,0,0],brightness:0,valid:false});
  let pc=null,dc=null,context=null,frame=0,timer=0,focusTimer=0,stopToken='',epoch=0,abort=null,avatar=null,avatarLoading=null,planner=null,closing=null,opening=null,outputPlaying=false,responseId='',itemId='',turnId='',stream=null,mediaOrigin=0,lastMediaTime=null;
  const drained=new Set();
  function emptyFacts(){return {connected:false,remoteTrack:false,audioStarted:false,audioStopped:false,samples:0,peak:0,model:'',providerError:false,peer:'new',ice:'new',gathering:'new',channel:'connecting',localCandidates:0,stopConfirmed:null,positiveSignalSamples:0,positiveRigSamples:0,positiveAfterFocus:0,focusDuringOutput:false,signalCleared:false,signal:zero(),observations:[]};}
  let facts=emptyFacts();
  async function post(value){const r=await fetch('/api/concierge-voice',{method:'POST',credentials:'same-origin',redirect:'error',headers:{'Content-Type':'application/json'},body:JSON.stringify(value),signal:AbortSignal.timeout(12000)}),data=await r.json();if(!r.ok||data.enabled===false)throw Error(data.message||data.error||'Native voice is unavailable.');return data;}
  function showFacts(){
    evidence.textContent='WebRTC connected: '+facts.connected+' · Remote audio track created: '+facts.remoteTrack+' · Provider audio started: '+facts.audioStarted+' · Provider audio stopped: '+facts.audioStopped+' · Audio samples: '+facts.samples+' · Peak normalized audio level: '+facts.peak.toFixed(4)+' · Peer: '+facts.peer+' · ICE: '+facts.ice+' · Gathering: '+facts.gathering+' · Local candidates: '+facts.localCandidates+' · Event channel: '+facts.channel+(facts.stopConfirmed===null?'':' · Server hangup confirmed: '+facts.stopConfirmed)+(facts.model?' · Model: '+facts.model:'')+(facts.providerError?' · Provider reported a session error.':'');
    const rig=avatar?.snapshot();if(rig)renderer.textContent=rig.mode==='webgl'?'Production WebGL avatar reported a rendered frame.':'Production avatar mode: '+rig.mode+'. GPU appearance and voice audibility are not established by this check.';
    signalEvidence.textContent=JSON.stringify({providerOutputTest:true,microphoneRequests:0,speechSignal:facts.signal,positiveSignalSamples:facts.positiveSignalSamples,positiveRigSamples:facts.positiveRigSamples,focusDuringOutput:facts.focusDuringOutput,positiveAfterFocus:facts.positiveAfterFocus,signalCleared:facts.signalCleared,rendererMode:rig?.mode||'unloaded',curveClosed:rig?.speechVisual?.curveClosed===true,rigRippleActive:rig?.speechVisual?.rippleActive===true,physicalAudibility:'not-measured',wordSynchronization:'not-measured',focusMeaning:'authored fixture via production rig; not a catalogue action',observations:facts.observations},null,2);
  }
  async function ensureAvatar(run){
    if(!avatarLoading)avatarLoading=new Promise((resolve,reject)=>{const script=document.createElement('script');script.src='/brites-concierge-avatar.js';script.onload=resolve;script.onerror=()=>reject(Error('The production avatar did not load.'));document.head.append(script);}).catch(error=>{avatarLoading=null;throw error;});
    await avatarLoading;if(run!==epoch)return false;
    if(!window.BritesConciergeAvatar||!window.BritesConciergeVoice?.measureOutputSignal||!window.BritesConciergeVoice?.hasVoiceNetworkRoute)throw Error('The current avatar and shared speech meter are required.');
    if(!avatar){avatar=window.BritesConciergeAvatar.create({container:document.querySelector('#avatar-mount'),assetBase:location.origin+'/',visible:true,greetingOnOpen:false});planner=window.BritesConciergeExpression.create({now:()=>performance.now(),onExpression:cue=>avatar.setExpression(cue)});}
    await avatar.ready;if(run!==epoch)return false;showFacts();return true;
  }
  function gather(peer,signal){if(peer.iceGatheringState==='complete')return Promise.resolve();return new Promise((resolve,reject)=>{const timeout=setTimeout(()=>finish(Error('The browser did not finish gathering voice connection candidates.')),10000);function finish(error){clearTimeout(timeout);peer.removeEventListener('icegatheringstatechange',change);signal.removeEventListener('abort',cancel);error?reject(error):resolve();}function change(){if(peer.iceGatheringState==='complete')finish();}function cancel(){finish(Error('Voice check cancelled.'));}peer.addEventListener('icegatheringstatechange',change);signal.addEventListener('abort',cancel,{once:true});if(signal.aborted)cancel();else change();});}
  function clearSignal(){facts.signal=zero();facts.signalCleared=true;level.value=0;avatar?.setSpeechSignal(zero());}
  function observe(mediaMs){const visual=avatar?.snapshot()?.speechVisual;if(visual?.rippleActive)facts.positiveRigSamples++;if(facts.observations.length<16&&(!facts.observations.length||mediaMs-facts.observations.at(-1).mediaMs>=350))facts.observations.push({mediaMs:Math.round(mediaMs),amplitude:facts.signal.amplitude,bands:[...facts.signal.bands],brightness:facts.signal.brightness,rigAmplitude:visual?.amplitude||0,curveClosed:visual?.curveClosed===true,rippleActive:visual?.rippleActive===true,fixtureTray:!fixtureTray.hidden});}
  function showFixture(){if(!outputPlaying||!focusOption.checked||facts.focusDuringOutput)return;fixtureTray.hidden=false;avatar?.focusProduct({x:-.35,y:.12});facts.focusDuringOutput=true;showFacts();}
  function receive(event){
    let e;try{e=JSON.parse(event.data);}catch{return;}
    if(e.type==='session.created')facts.model=e.session?.model||'';
    if(e.type==='response.created')responseId=String(e.response?.id||'').slice(0,200);
    if(e.response_id&&responseId&&e.response_id!==responseId)return;
    if(e.type==='response.output_audio_transcript.delta'||e.type==='response.output_audio_transcript.done'){
      itemId=String(e.item_id||itemId).slice(0,200);if(transcript.dataset.started!=='1'){transcript.textContent='';transcript.dataset.started='1';}
      if(e.type.endsWith('.done')&&e.transcript)transcript.textContent=e.transcript;else transcript.textContent+=e.delta||'';
      planner?.transcript({role:'assistant',text:transcript.textContent,responseId,itemId,inputItemId:turnId,currentTurn:true,contentIndex:e.content_index});
    }
    if(e.type==='output_audio_buffer.started'){
      if(drained.has(e.response_id||responseId))return;
      facts.audioStarted=true;outputPlaying=true;lastMediaTime=null;mediaOrigin=null;status.textContent='Receiving OpenAI’s native voice.';avatar?.setState('speaking');planner?.playback({playing:true,responseId,inputItemId:turnId,currentTurn:true});const run=epoch;focusTimer=setTimeout(()=>{if(run===epoch)showFixture();},500);
    }
    if(['output_audio_buffer.stopped','output_audio_buffer.cleared'].includes(e.type)){
      if(e.response_id||responseId)drained.add(e.response_id||responseId);if(drained.size>24)drained.delete(drained.values().next().value);
      facts.audioStopped=true;outputPlaying=false;clearTimeout(focusTimer);clearSignal();planner?.playback({playing:false,responseId,inputItemId:turnId,currentTurn:true,cleared:e.type.endsWith('.cleared')});avatar?.setState('idle');status.textContent='The native greeting finished. Ending the check…';const run=epoch;setTimeout(()=>{if(run===epoch)end('Native voice check ended.');},800);
    }
    if(e.type==='error'){facts.providerError=true;void end('The provider reported a voice-session error.');}
    showFacts();
  }
  function meterStream(remote,run){
    if(!context)return;const analyser=context.createAnalyser();analyser.fftSize=512;analyser.smoothingTimeConstant=0;const source=context.createMediaStreamSource(remote);source.connect(analyser);const samples=new Float32Array(analyser.fftSize),frequencies=new Float32Array(analyser.frequencyBinCount);
    function tick(){if(run!==epoch||stream!==remote)return;let next=zero();try{analyser.getFloatTimeDomainData(samples);const rms=window.BritesConciergeVoice.rms(samples);facts.peak=Math.max(facts.peak,rms);if(rms>.0001)facts.samples++;
      const time=Number(audio.currentTime),current=outputPlaying&&!!responseId&&!document.hidden&&!audio.paused&&!audio.ended&&pc?.connectionState==='connected'&&context?.state==='running'&&Number.isFinite(time)&&time>=0&&time<=600&&(lastMediaTime===null||time>=lastMediaTime);
      if(current){next=window.BritesConciergeVoice.measureOutputSignal(analyser,samples,frequencies,context.sampleRate);if(mediaOrigin===null)mediaOrigin=time;lastMediaTime=time;planner?.level({input:0,output:next.amplitude,outputTimeMs:Math.max(0,(time-mediaOrigin)*1000),responseId,inputItemId:turnId,currentTurn:true});}
      facts.signal=next;level.value=next.amplitude;avatar?.setSpeechSignal(next);
      if(next.valid&&next.amplitude>.015){facts.signalCleared=false;facts.positiveSignalSamples++;if(facts.focusDuringOutput)facts.positiveAfterFocus++;observe(Math.max(0,(time-mediaOrigin)*1000));}
    }catch{clearSignal();}showFacts();frame=requestAnimationFrame(tick);}tick();
  }
  function end(message){
    if(closing)return closing;
    const ended=++epoch,pending=opening;opening=null;abort?.abort();abort=null;clearTimeout(timer);clearTimeout(focusTimer);cancelAnimationFrame(frame);outputPlaying=false;clearSignal();planner?.cancel();avatar?.setState('idle');avatar?.clearFocus();if(dc)dc.close();if(pc)pc.close();dc=null;pc=null;stream=null;audio.pause();audio.srcObject=null;if(context){context.close().catch(()=>{});context=null;}let token=stopToken;stopToken='';start.disabled=true;stop.disabled=true;if(message)status.textContent=message;showFacts();
    closing=(async()=>{if(!token&&pending){try{token=(await pending)?.stopToken||'';}catch{}}if(token){try{const result=await post({action:'stop',stopToken:token});if(ended===epoch)facts.stopConfirmed=result.stopped===true;}catch{if(ended===epoch)facts.stopConfirmed=false;}}})().finally(()=>{if(ended===epoch){start.disabled=false;showFacts();}closing=null;});return closing;
  }
  start.addEventListener('click',async()=>{
    if(start.disabled||closing)return;
    const run=++epoch;abort=new AbortController();start.disabled=true;stop.disabled=false;status.textContent='Checking native voice availability…';transcript.textContent='';delete transcript.dataset.started;facts=emptyFacts();fixtureTray.hidden=true;outputPlaying=false;drained.clear();responseId=itemId='';turnId='native-qa-turn-'+run;showFacts();
    try{const AC=window.AudioContext||window.webkitAudioContext;if(!AC)throw Error('This browser cannot analyse received audio.');context=new AC();await context.resume();if(!await ensureAvatar(run))return;avatar.setState('thinking');planner.beginTurn({id:turnId,context:'ordinary'});
      const cap=await post({action:'capabilities'});if(run!==epoch)return;pc=new RTCPeerConnection();const peer=pc;peer.addTransceiver('audio',{direction:'recvonly'});
      function transport(){if(run!==epoch)return;facts.peer=peer.connectionState;facts.ice=peer.iceConnectionState;facts.gathering=peer.iceGatheringState;facts.channel=dc?.readyState||'closed';if(peer.connectionState==='connected')facts.connected=true;if(['disconnected','failed','closed'].includes(peer.connectionState))clearSignal();showFacts();}
      peer.addEventListener('connectionstatechange',transport);peer.addEventListener('iceconnectionstatechange',transport);peer.addEventListener('icegatheringstatechange',transport);peer.addEventListener('icecandidate',e=>{if(run===epoch&&e.candidate){facts.localCandidates++;showFacts();}});
      peer.ontrack=e=>{if(run!==epoch)return;cancelAnimationFrame(frame);facts.remoteTrack=true;stream=e.streams[0]||new MediaStream([e.track]);const remote=stream;lastMediaTime=null;mediaOrigin=null;audio.srcObject=remote;audio.play().catch(()=>{if(run===epoch&&stream===remote&&audio.srcObject===remote)void end('Native audio arrived, but browser playback was blocked. This check ended; typed chat remains available.');});meterStream(remote,run);showFacts();};
      dc=peer.createDataChannel('oai-events');dc.onmessage=e=>{if(run===epoch)receive(e);};dc.onopen=()=>{if(run!==epoch)return;transport();status.textContent='Connected. Requesting one short spoken greeting…';dc.send(JSON.stringify({type:'conversation.item.create',item:{id:turnId,type:'message',role:'user',content:[{type:'input_text',text:'Please introduce yourself as the Brites AI guide in three short friendly sentences. Say I can take my time, then ask if this is a gift or a piece for myself. Do not name products or give shop facts.'}]}}));dc.send(JSON.stringify({type:'response.create',response:{tool_choice:'none',max_output_tokens:300}}));};
      const offer=await peer.createOffer();if(run!==epoch)return;await peer.setLocalDescription(offer);if(run!==epoch)return;status.textContent='Preparing the browser’s voice connection…';await gather(peer,abort.signal);if(run!==epoch)return;const sdp=peer.localDescription.sdp;if(!window.BritesConciergeVoice.hasVoiceNetworkRoute(sdp))throw Error('This browser exposed no voice network route. No provider call was started.');status.textContent='Connecting to OpenAI’s native voice…';opening=post({action:'start',sdp,demoToken:cap.demoToken||''});const call=await opening;opening=null;if(run!==epoch)return;stopToken=call.stopToken;await peer.setRemoteDescription({type:'answer',sdp:call.sdp});if(run!==epoch)return;transport();timer=setTimeout(()=>end(facts.connected?'The 20-second connection check ended.':'The browser’s voice transport did not connect within 20 seconds.'),20000);
    }catch(error){if(run===epoch)await end(error.message||'Native voice could not connect.');}
  });
  stop.addEventListener('click',()=>end('Voice check ended.'));
  document.addEventListener('visibilitychange',()=>{if(document.hidden)void end('Voice check ended when the page became hidden.');});
  window.addEventListener('pagehide',()=>end());
})();
