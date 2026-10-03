(function(root,factory){'use strict';const api=factory();if(typeof module==='object'&&module.exports)module.exports=api;else root.BritesConciergeVoice=api;})(typeof window!=='undefined'?window:globalThis,function(){'use strict';
  // Native OpenAI speech-to-speech over WebRTC. Construction never requests a
  // microphone, calls a model or invokes browser speech synthesis/recognition.
  function rms(samples){let sum=0;for(let i=0;i<samples.length;i++)sum+=samples[i]*samples[i];return samples.length?Math.min(1,Math.sqrt(sum/samples.length)*4):0;}
  function validateToolArguments(value){if(!value||typeof value!=='object'||Array.isArray(value)||Object.keys(value).some(k=>k!=='message')||typeof value.message!=='string'||!value.message.trim()||value.message.length>2000||/[\u0000-\u0008\u000b\u000c\u000e-\u001f]/.test(value.message))return null;return {message:value.message.trim()};}
  function create(options={}){
    const rt=options.runtime||globalThis,doc=rt.document,nav=rt.navigator,endpoint=options.endpoint||'/api/concierge-voice';
    const ownOrigin=rt.location?.origin||'https://preview.invalid';
    if(new URL(endpoint,ownOrigin).origin!==ownOrigin)throw Error('Voice endpoint must be on this website.');
    const notify=(key,...args)=>{try{if(typeof options[key]==='function')options[key](...args);}catch{}};
    let epoch=0,turnVersion=0,state='idle',disposed=false,pc=null,dc=null,mic=null,audio=null,ctx=null,raf=null,deadline=null,abort=null,stopCredential=null,closing=null,outputPlaying=false,inputSpeaking=false,responsePending=false;
    let inputMeter=null,outputMeter=null;const sources=[],timers=new Set(),pending=new Set(),toolCalls=new Set(),toolControllers=new Set();
    function timeout(ms,fn){const id=rt.setTimeout(()=>{timers.delete(id);fn();},ms);timers.add(id);return id;}
    function clear(id){if(id!=null){rt.clearTimeout(id);timers.delete(id);}}
    function setState(next){if(state===next)return;state=next;notify('onState',next);}
    function send(value){if(dc?.readyState==='open'){dc.send(JSON.stringify(value));return true;}return false;}
    function bounded(promise,ms,label){return new Promise((resolve,reject)=>{let finished=false;const complete=(fn,value)=>{if(finished)return;finished=true;clear(id);pending.delete(cancel);fn(value);};const cancel=()=>complete(reject,Error('Voice operation cancelled.'));const id=timeout(ms,()=>complete(reject,Error(label)));pending.add(cancel);Promise.resolve(promise).then(value=>complete(resolve,value),error=>complete(reject,error));});}
    async function gatherIce(connection,current){
      if(current!==epoch||disposed)throw Error('Voice start cancelled.');
      if(connection.iceGatheringState==='complete')return;
      // This connection posts one SDP offer rather than trickling candidates.
      // Wait for the browser's local SDP to contain its gathered candidates.
      // The existing bounded operation cancels on End/Hide/dispose and always
      // removes the temporary event listener, including timeout/error paths.
      let ready;const completed=new Promise(resolve=>{ready=resolve;});
      const onState=()=>{if(connection.iceGatheringState==='complete')ready();};
      connection.addEventListener('icegatheringstatechange',onState);
      try{onState();await bounded(completed,10000,'OpenAI voice network setup timed out while gathering connection candidates.');}
      finally{connection.removeEventListener('icegatheringstatechange',onState);}
    }
    async function request(body,{signal,keepalive=false}={}){
      const headers={...(typeof options.headers==='function'?options.headers():options.headers||{}),'Content-Type':'application/json'};
      const response=await rt.fetch(endpoint,{method:'POST',headers,credentials:'same-origin',body:JSON.stringify(body),signal,keepalive});let data;
      try{data=await response.json();}catch{throw Error('OpenAI voice returned an invalid connection answer.');}
      if(!response.ok||data.enabled===false){const error=Error(data.message||data.error||'OpenAI voice is unavailable. You can still type.');error.code=data.code||'';throw error;}return data;
    }
    function cleanup(){
      for(const controller of toolControllers)controller.abort();toolControllers.clear();
      abort?.abort();abort=null;for(const cancel of [...pending])cancel();clear(deadline);deadline=null;
      if(raf!=null){rt.cancelAnimationFrame?.(raf);raf=null;}
      for(const id of timers)rt.clearTimeout(id);timers.clear();
      mic?.getTracks().forEach(track=>track.stop());mic=null;
      if(audio){audio.pause();audio.srcObject=null;audio.remove?.();audio=null;}
      sources.forEach(node=>{try{node.disconnect();}catch{}});sources.length=0;
      if(ctx){ctx.close().catch(()=>{});ctx=null;}
      if(dc){dc.onopen=dc.onmessage=dc.onerror=dc.onclose=null;dc.close();dc=null;}
      if(pc){pc.ontrack=pc.onconnectionstatechange=null;pc.close();pc=null;}
      inputMeter=outputMeter=null;outputPlaying=inputSpeaking=responsePending=false;toolCalls.clear();notify('onLevel',{input:0,output:0});
    }
    function meter(stream,channel){if(!ctx)return null;const source=ctx.createMediaStreamSource(stream),analyser=ctx.createAnalyser();analyser.fftSize=512;source.connect(analyser);sources.push(source,analyser);return {channel,analyser,samples:new Float32Array(analyser.fftSize)};}
    function sample(){if(disposed||!ctx)return;const levels={input:0,output:0};for(const value of [inputMeter,outputMeter])if(value){value.analyser.getFloatTimeDomainData(value.samples);levels[value.channel]=rms(value.samples);}notify('onLevel',levels);raf=rt.requestAnimationFrame?.(sample);}
    function settleState(){if(state==='idle'||state==='closing')return;if(outputPlaying)setState('speaking');else if(inputSpeaking)setState('listening');else if(toolControllers.size||responsePending)setState('thinking');else setState('listening');}
    async function executeTool(event,current){
      const callId=event.call_id;if(typeof callId!=='string'||callId.length>200||toolCalls.has(callId))return;
      toolCalls.add(callId);if(toolCalls.size>100){await stop('limit');return;}
      const version=turnVersion,controller=new rt.AbortController();toolControllers.add(controller);setState('thinking');let result;
      try{
        let args;try{args=validateToolArguments(JSON.parse(event.arguments||''));}catch{}
        if(event.name!=='find_jewellery'||!args||typeof options.onTool!=='function')throw Error('Tool unavailable.');
        result=await bounded(options.onTool(args,{signal:controller.signal}),14000,'The catalogue check timed out.');
        // Only the existing public, current, checked catalogue projection is
        // supplied by the host. Operator dossiers and credentials stay private.
        if(!result||typeof result!=='object'||Array.isArray(result))throw Error('Catalogue answer unavailable.');
        const text=JSON.stringify(result);if(text.length>30000)throw Error('Catalogue result too large.');result=text;
      }catch{result=JSON.stringify({verified:false,message:'The current catalogue could not be checked. Please use the visible shop controls or try again. Do not recommend unverified products.'});}
      finally{controller.abort();toolControllers.delete(controller);}
      if(current!==epoch||disposed||state==='closing'||state==='idle')return;
      send({type:'conversation.item.create',item:{type:'function_call_output',call_id:callId,output:result}});
      // An interrupted lookup may finish, but must not start speaking over the
      // shopper's new turn. The next response can use the checked tool result.
      if(version===turnVersion&&!inputSpeaking){responsePending=true;send({type:'response.create',response:{tool_choice:'none'}});}settleState();
    }
    function receive(raw,current){
      let event;try{if(typeof raw!=='string'||raw.length>65000)return;event=JSON.parse(raw);}catch{return;}
      if(!event||typeof event.type!=='string'||current!==epoch||state==='closing'||state==='idle')return;
      if(event.type==='input_audio_buffer.speech_started'){inputSpeaking=true;interrupt();setState('listening');}
      else if(event.type==='input_audio_buffer.speech_stopped'){inputSpeaking=false;responsePending=true;setState('thinking');}
      else if(event.type==='response.created'){responsePending=true;settleState();}
      else if(event.type==='output_audio_buffer.started'){outputPlaying=true;setState('speaking');}
      else if(event.type==='output_audio_buffer.stopped'||event.type==='output_audio_buffer.cleared'){outputPlaying=false;settleState();}
      else if(event.type==='response.done'){
        responsePending=false;
        // Generation can finish while WebRTC is still playing buffered audio.
        // Do not freeze the talking pose before output_audio_buffer.stopped.
        if(event.response?.status==='failed')notify('onError','OpenAI voice could not finish that reply. You can interrupt, retry or type.');settleState();
      }
      else if(event.type==='response.function_call_arguments.done')void executeTool(event,current);
      else if(event.type==='response.output_audio_transcript.delta'||event.type==='response.audio_transcript.delta')notify('onTranscript',{role:'assistant',delta:typeof event.delta==='string'?event.delta.slice(0,2000):'',final:false,itemId:event.item_id||event.response_id||''});
      else if(event.type==='response.output_audio_transcript.done'||event.type==='response.audio_transcript.done')notify('onTranscript',{role:'assistant',text:typeof event.transcript==='string'?event.transcript.slice(0,8000):'',final:true,itemId:event.item_id||event.response_id||''});
      else if(event.type==='conversation.item.input_audio_transcription.completed')notify('onTranscript',{role:'user',text:typeof event.transcript==='string'?event.transcript.slice(0,2000):'',final:true,itemId:event.item_id||''});
      else if(event.type==='error'){
        // response.cancel during silence can yield a harmless race. Raw
        // provider messages may contain account details and never reach the UI.
        if(event.error?.code!=='response_cancel_not_active')notify('onError','OpenAI voice could not finish that turn. You can interrupt, stop or type.');
      }
    }
    function interrupt(){++turnVersion;for(const controller of toolControllers)controller.abort();toolControllers.clear();send({type:'response.cancel'});send({type:'output_audio_buffer.clear'});outputPlaying=false;responsePending=false;notify('onLevel',{input:0,output:0});if(state!=='closing'&&state!=='idle')setState('listening');}
    async function stopLateAnswer(answer){
      if(typeof answer?.stopToken!=='string'||answer.stopToken.length>1200)return;
      const controller=new rt.AbortController(),id=rt.setTimeout(()=>controller.abort(),5500);
      try{await request({action:'stop',stopToken:answer.stopToken},{signal:controller.signal,keepalive:true});}catch{/* The durable server deadline remains the independent backup. */}finally{rt.clearTimeout(id);}
    }
    async function start(){
      if(disposed)throw Error('Voice adapter is closed.');if(state!=='idle')return false;
      const current=++epoch;++turnVersion;setState('connecting');abort=new rt.AbortController();
      try{
        if(!rt.RTCPeerConnection||!nav?.mediaDevices?.getUserMedia)throw Error('This browser cannot start OpenAI voice. Try a browser with microphone access, or type here.');
        // Check explicit sandbox allocation/provider configuration before
        // requesting access to the shopper's microphone.
        const capabilities=await bounded(request({action:'capabilities'},{signal:abort.signal}),12000,'OpenAI voice availability check timed out.');
        if(current!==epoch)throw Error('Voice start cancelled.');
        const gum=nav.mediaDevices.getUserMedia({audio:{echoCancellation:true,noiseSuppression:true,autoGainControl:true},video:false});
        gum.then(stream=>{if(current!==epoch||disposed)stream.getTracks().forEach(track=>track.stop());},()=>{});
        mic=await bounded(gum,20000,'Microphone permission timed out.');if(current!==epoch)throw Error('Voice start cancelled.');
        pc=new rt.RTCPeerConnection();audio=doc.createElement('audio');audio.autoplay=true;audio.setAttribute('aria-hidden','true');audio.hidden=true;doc.body?.appendChild(audio);
        const AudioContext=rt.AudioContext||rt.webkitAudioContext;if(AudioContext){ctx=new AudioContext();await bounded(ctx.resume(),5000,'Voice audio initialization timed out.');inputMeter=meter(mic,'input');sample();}
        pc.ontrack=event=>{if(current!==epoch)return;const stream=event.streams?.[0]||(rt.MediaStream?new rt.MediaStream([event.track]):null);if(!stream)return;audio.srcObject=stream;outputMeter=meter(stream,'output');audio.play().catch(()=>notify('onError','Your browser paused OpenAI audio. End voice and start again to allow playback.'));};
        mic.getAudioTracks().forEach(track=>pc.addTrack(track,mic));
        dc=pc.createDataChannel('oai-events');dc.onmessage=event=>receive(event.data,current);dc.onerror=()=>{notify('onError','OpenAI voice disconnected. You can still type.');void stop('connection');};
        pc.onconnectionstatechange=()=>{if(['failed','closed'].includes(pc?.connectionState)&&state!=='closing')void stop('connection');};
        const ready=new Promise((resolve,reject)=>{dc.onopen=resolve;dc.onclose=()=>{if(state==='connecting')reject(Error('OpenAI voice connection closed.'));else if(state!=='closing'&&state!=='idle')void stop('connection');};});ready.catch(()=>{});
        const offer=await bounded(pc.createOffer(),5000,'Voice negotiation timed out.');await bounded(pc.setLocalDescription(offer),5000,'Voice negotiation timed out.');
        await gatherIce(pc,current);if(current!==epoch)throw Error('Voice start cancelled.');
        const opening=request({action:'start',sdp:pc.localDescription?.sdp||offer.sdp,...(capabilities.demoToken?{demoToken:capabilities.demoToken}:{})},{signal:abort.signal});
        opening.then(answer=>{if(current!==epoch)void stopLateAnswer(answer);},()=>{});
        const answer=await bounded(opening,15000,'OpenAI voice setup timed out.');
        if(current!==epoch)throw Error('Voice start cancelled.');
        if(typeof answer.sdp!=='string'||!/^v=0\r?\n/.test(answer.sdp)||typeof answer.stopToken!=='string')throw Error('OpenAI voice answer could not be verified.');
        stopCredential=answer.stopToken;
        await bounded(pc.setRemoteDescription({type:'answer',sdp:answer.sdp}),5000,'Voice negotiation timed out.');await bounded(ready,10000,'OpenAI voice media did not connect.');
        if(current!==epoch)throw Error('Voice start cancelled.');
        const duration=Math.min(120000,Math.max(1000,Number(answer.maxDurationMs)||120000),Number.isFinite(answer.expiresAt)?Math.max(0,answer.expiresAt-Date.now()):120000);
        deadline=timeout(duration,()=>void stop('limit'));setState('listening');
        if(options.greeting!==false){responsePending=true;send({type:'response.create',response:{instructions:'Greet the shopper warmly in one short sentence, then ask whether this is a piece for them or a gift. Do not name products or promise any shop facts yet. Speak as the Brites AI concierge, with a relaxed natural voice.',tool_choice:'none',max_output_tokens:90}});settleState();}
        return true;
      }catch(error){if(current===epoch){notify('onError',error?.message||'OpenAI voice could not connect. You can still type.');await stop('failed');}return false;}
    }
    function stop(reason='user'){
      if(closing)return closing;if(state==='idle'){cleanup();return Promise.resolve();}
      const token=stopCredential;stopCredential=null;setState('closing');++epoch;abort?.abort();mic?.getTracks().forEach(track=>track.stop());interrupt();cleanup();
      // WebRTC cancellation clears output immediately. The server hangs up the
      // exact signed call; its independently recorded deadline remains a backup.
      closing=(async()=>{if(token){try{const result=await bounded(request({action:'stop',stopToken:token},{keepalive:true}),5500,'Voice stop confirmation timed out.');notify('onClose',{serverStopped:result.stopped===true});}catch{notify('onClose',{serverStopped:false});}}cleanup();setState('idle');notify('onStopped',reason);})().finally(()=>{closing=null;});return closing;
    }
    const onHidden=()=>{if(doc?.hidden)void stop('hidden');},onPageHide=()=>void stop('pagehide');
    doc?.addEventListener('visibilitychange',onHidden);rt.addEventListener?.('pagehide',onPageHide);
    async function dispose(){disposed=true;doc?.removeEventListener('visibilitychange',onHidden);rt.removeEventListener?.('pagehide',onPageHide);await stop('disposed');}
    return {start,stop,cancel:stop,interrupt,dispose,get state(){return state;}};
  }
  return {create,rms,validateToolArguments};
});
