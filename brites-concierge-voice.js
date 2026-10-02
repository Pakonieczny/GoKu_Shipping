(function(root,factory){'use strict';const api=factory();if(typeof module==='object'&&module.exports)module.exports=api;else root.BritesConciergeVoice=api;})(typeof window!=='undefined'?window:globalThis,function(){'use strict';
  // Opt-in only: constructing this adapter never asks for a microphone or
  // starts inference. Audio levels come from actual media samples, not timers.
  function rms(samples){let sum=0;for(let i=0;i<samples.length;i++)sum+=samples[i]*samples[i];return samples.length?Math.min(1,Math.sqrt(sum/samples.length)*4):0;}
  function validateToolArguments(value){if(!value||typeof value!=='object'||Array.isArray(value)||Object.keys(value).some(k=>k!=='message')||typeof value.message!=='string'||!value.message.trim()||value.message.length>2000||/[\u0000-\u0008\u000b\u000c\u000e-\u001f]/.test(value.message))return null;return {message:value.message.trim()};}
  function create(options={}){
    const rt=options.runtime||globalThis,doc=rt.document,nav=rt.navigator;
    const endpoint=options.endpoint||'/api/concierge-voice',demoEndpoint=options.demoEndpoint||'/api/concierge-demo-turn';
    const ownOrigin=rt.location?.origin||'https://preview.invalid';
    if(new URL(endpoint,ownOrigin).origin!==ownOrigin||new URL(demoEndpoint,ownOrigin).origin!==ownOrigin)throw Error('Voice endpoint must be on this website.');
    const notify=(key,...args)=>{try{if(typeof options[key]==='function')options[key](...args);}catch{}};
    let epoch=0,state='idle',disposed=false,pc=null,dc=null,mic=null,audio=null,ctx=null,raf=null,deadline=null,abort=null,stopCredential=null,closing=null,closeResolve=null,closeTimer=null,toolCalls=new Set(),sources=[],recognition=null,demoMode=false,demoTurns=0,demoHistory=[],demoSpeaking=false;
    const timers=new Set(),pending=new Set();
    function timeout(ms,fn){const id=rt.setTimeout(()=>{timers.delete(id);fn();},ms);timers.add(id);return id;}
    function clear(id){if(id!=null){rt.clearTimeout(id);timers.delete(id);}}
    function setState(next){state=next;notify('onState',next);}
    function send(value){if(dc?.readyState==='open'){dc.send(JSON.stringify(value));return true;}return false;}
    function bounded(promise,ms,label){return new Promise((resolve,reject)=>{let finished=false;const complete=(fn,value)=>{if(finished)return;finished=true;clear(id);pending.delete(cancel);fn(value);};const cancel=()=>complete(reject,Error('Voice operation cancelled.'));const id=timeout(ms,()=>complete(reject,Error(label)));pending.add(cancel);Promise.resolve(promise).then(value=>complete(resolve,value),error=>complete(reject,error));});}
    async function post(target,body,{signal,keepalive=false}={}){const headers={...(typeof options.headers==='function'?options.headers():options.headers||{}),'Content-Type':'application/json'};const response=await rt.fetch(target,{method:'POST',headers,credentials:'same-origin',body:JSON.stringify(body),signal,keepalive});let data;try{data=await response.json();}catch{throw Error('Voice connection returned an invalid answer.');}if(!response.ok||data.enabled===false){const error=Error(data.message||data.error||'Live voice is unavailable. Text remains available.');error.code=data.code||'';throw error;}return data;}
    const request=(body,config)=>post(endpoint,body,config),demoRequest=(body,config)=>post(demoEndpoint,body,config);
    function cleanup(){
      abort?.abort();abort=null;for(const cancel of [...pending])cancel();clear(deadline);deadline=null;clear(closeTimer);closeTimer=null;
      if(raf!=null){rt.cancelAnimationFrame?.(raf);raf=null;}
      try{recognition?.abort?.();}catch{}recognition=null;demoMode=false;demoSpeaking=false;demoTurns=0;demoHistory=[];
      try{rt.speechSynthesis?.cancel?.();}catch{}
      for(const id of timers)rt.clearTimeout(id);timers.clear();
      mic?.getTracks().forEach(track=>track.stop());mic=null;
      if(audio){audio.pause();audio.srcObject=null;audio.remove?.();audio=null;}
      sources.forEach(node=>{try{node.disconnect();}catch{}});sources=[];
      if(ctx){ctx.close().catch(()=>{});ctx=null;}
      if(dc){dc.onopen=dc.onmessage=dc.onerror=dc.onclose=null;dc.close();dc=null;}
      if(pc){pc.ontrack=pc.onconnectionstatechange=null;pc.close();pc=null;}
      toolCalls.clear();notify('onLevel',{input:0,output:0});
      if(closeResolve){closeResolve();closeResolve=null;}
    }
    function meter(stream,channel){if(!ctx)return null;const source=ctx.createMediaStreamSource(stream),analyser=ctx.createAnalyser();analyser.fftSize=512;source.connect(analyser);sources.push(source,analyser);return {channel,analyser,samples:new Float32Array(analyser.fftSize)};}
    let inputMeter=null,outputMeter=null;
    function sample(){if(disposed||!ctx)return;const levels={input:0,output:0};for(const value of [inputMeter,outputMeter])if(value){value.analyser.getFloatTimeDomainData(value.samples);levels[value.channel]=rms(value.samples);}notify('onLevel',levels);raf=rt.requestAnimationFrame?.(sample);}
    async function executeTool(event,current){
      const callId=event.call_id;if(typeof callId!=='string'||callId.length>200||toolCalls.has(callId))return;
      toolCalls.add(callId);if(toolCalls.size>100){await stop('limit');return;}
      let result;try{
        let args;try{args=validateToolArguments(JSON.parse(event.arguments||''));}catch{}
        if(event.name!=='find_jewellery'||!args||typeof options.onTool!=='function')throw Error('Tool unavailable.');
        setState('thinking');result=await bounded(options.onTool(args),14000,'The catalogue check timed out.');
        // The host must return its existing PUBLIC checked catalogue projection.
        // Private dossiers and operator review packets must never be supplied.
        if(!result||typeof result!=='object'||Array.isArray(result))throw Error('Catalogue answer unavailable.');
        const text=JSON.stringify(result);if(text.length>30000)throw Error('Catalogue result too large.');
        result=text;
      }catch{result=JSON.stringify({verified:false,message:'The current catalogue could not be checked. Please use the visible shop controls or try again. Do not recommend unverified products.'});}
      if(current!==epoch||disposed||state==='closing'||state==='idle')return;
      send({type:'conversation.item.create',item:{type:'function_call_output',call_id:callId,output:result}});send({type:'response.create',response:{tool_choice:'none'}});
    }
    function receive(raw,current){
      let event;try{if(typeof raw!=='string'||raw.length>65000)return;event=JSON.parse(raw);}catch{return;}
      if(!event||typeof event.type!=='string')return;
      if(event.type==='session.closed'){notify('onClose',{finalized:true});if(closeResolve)closeResolve();else if(current===epoch)void stop('provider');return;}
      if(current!==epoch)return;
      if(event.type==='input_audio_buffer.speech_started'){interrupt();setState('listening');}
      else if(event.type==='input_audio_buffer.speech_stopped')setState('thinking');
      else if(event.type==='output_audio_buffer.started')setState('speaking');
      else if(event.type==='output_audio_buffer.stopped'||event.type==='response.done'){if(state!=='closing')setState('listening');}
      else if(event.type==='response.function_call_arguments.done')void executeTool(event,current);
      else if(event.type==='response.output_audio_transcript.delta'||event.type==='response.audio_transcript.delta')notify('onTranscript',{role:'assistant',delta:typeof event.delta==='string'?event.delta.slice(0,2000):'',final:false});
      else if(event.type==='response.output_audio_transcript.done'||event.type==='response.audio_transcript.done')notify('onTranscript',{role:'assistant',text:typeof event.transcript==='string'?event.transcript.slice(0,8000):'',final:true});
      else if(event.type==='conversation.item.input_audio_transcription.completed')notify('onTranscript',{role:'user',text:typeof event.transcript==='string'?event.transcript.slice(0,2000):'',final:true});
      else if(event.type==='error')notify('onError','Voice could not finish that turn. You can interrupt, stop or use text.');
    }
    function interrupt(){send({type:'response.cancel'});send({type:'output_audio_buffer.clear'});try{rt.speechSynthesis?.cancel?.();}catch{}demoSpeaking=false;notify('onLevel',{input:0,output:0});if(demoMode&&state!=='closing'&&state!=='idle')listenDemo();}
    function listenDemo(){if(!demoMode||disposed||state==='closing'||state==='idle'||demoSpeaking||!recognition)return;try{recognition.start();setState('listening');}catch(error){if(error?.name!=='InvalidStateError')notify('onError','Voice listening could not restart. Select Talk to me to retry.');}}
    async function demoTurn(message,current){
      message=String(message||'').trim().slice(0,600);if(!message||current!==epoch||!demoMode)return;
      if(++demoTurns>12){await stop('limit');return;}
      setState('thinking');notify('onTranscript',{role:'user',text:message,final:true,spokenOnly:true});
      let catalogue;try{catalogue=await bounded(options.onTool?.({message}),14000,'The catalogue check timed out.');if(!catalogue||typeof catalogue!=='object'||Array.isArray(catalogue)||JSON.stringify(catalogue).length>20000)throw Error('Catalogue answer unavailable.');}catch{catalogue={products:[],meanings:[],question:'Would you tell me a little more about the person, occasion, or style you have in mind?'};}
      if(current!==epoch||!demoMode||state==='closing')return;
      let answer;try{answer=await bounded(demoRequest({action:'turn',message,history:demoHistory.slice(-6),catalogue},{signal:abort?.signal}),14000,'The voice answer timed out.');}catch(error){notify('onError',error?.message||'Voice could not answer. You can keep typing.');setState('listening');listenDemo();return;}
      const speech=typeof answer.speech==='string'?answer.speech.trim().slice(0,1200):'';if(!speech){setState('listening');listenDemo();return;}
      demoHistory.push({role:'user',content:message},{role:'assistant',content:speech});demoHistory=demoHistory.slice(-6);notify('onTranscript',{role:'assistant',text:speech,final:true,spokenOnly:true});
      const Utterance=rt.SpeechSynthesisUtterance;if(!Utterance||!rt.speechSynthesis?.speak){notify('onError','Spoken replies are unavailable in this browser. You can keep typing.');setState('listening');listenDemo();return;}
      const utterance=new Utterance(speech);utterance.rate=1;utterance.pitch=1.04;utterance.onstart=()=>{if(current===epoch&&demoMode){demoSpeaking=true;setState('speaking');notify('onLevel',{input:0,output:.55});}};utterance.onboundary=()=>{if(current===epoch&&demoMode)notify('onLevel',{input:0,output:.72});};utterance.onend=utterance.onerror=()=>{if(current!==epoch||!demoMode)return;demoSpeaking=false;notify('onLevel',{input:0,output:0});setState('listening');listenDemo();};
      try{rt.speechSynthesis.cancel();rt.speechSynthesis.speak(utterance);}catch{utterance.onerror();}
    }
    async function startDemo(current){
      const Recognition=rt.SpeechRecognition||rt.webkitSpeechRecognition;
      if(!Recognition||!rt.speechSynthesis||!rt.SpeechSynthesisUtterance)return false;
      let available;try{available=await bounded(demoRequest({action:'capabilities'},{signal:abort?.signal}),10000,'Voice demo availability check timed out.');}catch{return false;}
      if(!available?.enabled||current!==epoch)return false;
      recognition=new Recognition();recognition.lang=doc?.documentElement?.lang||'en-US';recognition.continuous=false;recognition.interimResults=true;recognition.maxAlternatives=1;demoMode=true;
      recognition.onresult=event=>{if(current!==epoch||!demoMode)return;let final='';for(let i=event.resultIndex||0;i<event.results.length;i++)if(event.results[i].isFinal)final+=event.results[i][0]?.transcript||'';if(final.trim())void demoTurn(final,current);};
      recognition.onerror=event=>{if(current!==epoch||state==='closing'||state==='idle')return;const denied=['not-allowed','service-not-allowed'].includes(event.error);notify('onError',denied?'Microphone permission was not granted. You can keep typing.':'Voice listening paused. Select Talk to me to retry.');void stop(denied?'permission':'recognition');};
      recognition.onend=()=>{if(current===epoch&&demoMode&&!demoSpeaking&&state==='listening')timeout(120,listenDemo);};
      deadline=timeout(Math.min(120000,Math.max(1000,Number(available.maxDurationMs)||120000)),()=>void stop('limit'));listenDemo();return true;
    }
    async function start(){
      if(disposed)throw Error('Voice adapter is closed.');if(state!=='idle')return false;
      const current=++epoch;setState('connecting');abort=new rt.AbortController();
      try{
        // Check disabled/provider/auth state BEFORE requesting the microphone.
        if(!rt.RTCPeerConnection||!nav?.mediaDevices?.getUserMedia){if(await startDemo(current))return true;throw Error('Live voice is not supported here. Text and narration remain available.');}
        try{await bounded(request({action:'capabilities'},{signal:abort.signal}),12000,'Voice availability check timed out.');}catch(error){if(await startDemo(current))return true;throw error;}
        if(current!==epoch)throw Error('Voice start cancelled.');
        const gum=nav.mediaDevices.getUserMedia({audio:{echoCancellation:true,noiseSuppression:true,autoGainControl:true},video:false});
        gum.then(stream=>{if(current!==epoch||disposed)stream.getTracks().forEach(track=>track.stop());},()=>{});
        mic=await bounded(gum,20000,'Microphone permission timed out.');if(current!==epoch)throw Error('Voice start cancelled.');
        pc=new rt.RTCPeerConnection();audio=doc.createElement('audio');audio.autoplay=true;audio.setAttribute('aria-hidden','true');audio.hidden=true;doc.body?.appendChild(audio);
        const AudioContext=rt.AudioContext||rt.webkitAudioContext;if(AudioContext){ctx=new AudioContext();await bounded(ctx.resume(),5000,'Voice audio initialization timed out.');inputMeter=meter(mic,'input');sample();}
        pc.ontrack=event=>{if(current!==epoch)return;const stream=event.streams?.[0]||(rt.MediaStream?new rt.MediaStream([event.track]):null);if(!stream)return;audio.srcObject=stream;outputMeter=meter(stream,'output');audio.play().catch(()=>notify('onError','Select voice again if your browser pauses audio playback.'));};
        mic.getAudioTracks().forEach(track=>pc.addTrack(track,mic));
        dc=pc.createDataChannel('oai-events');dc.onmessage=event=>receive(event.data,current);dc.onerror=()=>{notify('onError','Voice disconnected. Text remains available.');void stop('connection');};
        pc.onconnectionstatechange=()=>{if(['failed','disconnected','closed'].includes(pc?.connectionState)&&state!=='closing')void stop('connection');};
        const ready=new Promise((resolve,reject)=>{dc.onopen=resolve;dc.onclose=()=>{if(state==='connecting')reject(Error('Voice connection closed.'));};});
        ready.catch(()=>{});
        const offer=await bounded(pc.createOffer(),5000,'Voice negotiation timed out.');await bounded(pc.setLocalDescription(offer),5000,'Voice negotiation timed out.');
        const answer=await bounded(request({action:'start',sdp:pc.localDescription?.sdp||offer.sdp},{signal:abort.signal}),15000,'Voice setup timed out.');
        if(current!==epoch)throw Error('Voice start cancelled.');
        if(typeof answer.sdp!=='string'||!/^v=0\r?\n/.test(answer.sdp)||typeof answer.stopToken!=='string')throw Error('Voice answer could not be verified.');
        stopCredential=answer.stopToken;
        await bounded(pc.setRemoteDescription({type:'answer',sdp:answer.sdp}),5000,'Voice negotiation timed out.');await bounded(ready,10000,'Voice media did not connect.');
        if(current!==epoch)throw Error('Voice start cancelled.');
        deadline=timeout(Math.min(120000,Math.max(1000,Number(answer.maxDurationMs)||120000)),()=>void stop('limit'));
        setState('listening');return true;
      }catch(error){if(current===epoch){notify('onError',error?.message||'Voice could not connect. Text remains available.');await stop('failed');}return false;}
    }
    function stop(reason='user'){
      if(closing)return closing;if(state==='idle'){cleanup();return Promise.resolve();}
      const wasOpen=dc?.readyState==='open';setState('closing');++epoch;
      // Stop capturing immediately. Retain event/audio transport briefly for
      // graceful provider finalization; never leave the microphone running.
      mic?.getTracks().forEach(track=>track.stop());interrupt();
      closing=(async()=>{
        const token=stopCredential;stopCredential=null;
        const finalize=wasOpen?new Promise(resolve=>{closeResolve=resolve;closeTimer=timeout(1500,()=>{notify('onClose',{finalized:false,reason:'deadline'});resolve();});send({type:'session.close'});}):Promise.resolve();
        const hangup=token?bounded(request({action:'stop',stopToken:token},{keepalive:true}),5500,'Voice stop confirmation timed out.').then(result=>notify('onClose',{serverStopped:result.stopped===true}),()=>notify('onClose',{serverStopped:false})):Promise.resolve();
        await Promise.all([finalize,hangup]);cleanup();inputMeter=outputMeter=null;setState('idle');notify('onStopped',reason);
      })().finally(()=>{closing=null;});return closing;
    }
    const onHidden=()=>{if(doc?.hidden)void stop('hidden');},onPageHide=()=>void stop('pagehide');
    doc?.addEventListener('visibilitychange',onHidden);rt.addEventListener?.('pagehide',onPageHide);
    async function dispose(){disposed=true;doc?.removeEventListener('visibilitychange',onHidden);rt.removeEventListener?.('pagehide',onPageHide);await stop('disposed');}
    return {start,stop,cancel:stop,interrupt,dispose,get state(){return state;}};
  }
  return {create,rms,validateToolArguments};
});
