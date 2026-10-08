'use strict';
// The real voice adapter with synthetic transport/device events. These checks
// establish binding and cancellation, not audible playback or word alignment.
const test=require('node:test'),assert=require('node:assert/strict');
const voiceApi=require('../../brites-concierge-voice.js');
const SDP='v=0\r\nm=audio 9 UDP/TLS/RTP/SAVPF 111\r\na=candidate:1 1 UDP 2122260223 192.0.2.10 50000 typ host\r\n';
function fixture(t,{mediaClock=false}={}){
  let channel,peer,serial=0,frameSerial=0;const sent=[],requests=[],transcripts=[],listening=[],playback=[],speech=[],calls=[],levels=[],audios=[],timers=new Map(),listeners=new Map(),frames=new Map();
  const track={kind:'audio',readyState:'live',stop(){}},stream={getAudioTracks:()=>[track],getTracks:()=>[track]};
  const document={hidden:false,body:{appendChild(){}},createElement(){const value={currentTime:0,paused:true,ended:false,setAttribute(){},play(){this.paused=false;return Promise.resolve();},pause(){this.paused=true;},remove(){}};audios.push(value);return value;},addEventListener:(key,fn)=>listeners.set(key,fn),removeEventListener:key=>listeners.delete(key)};
  class Peer{
    constructor(){peer=this;this.connectionState='new';this.iceGatheringState='complete';}addTrack(){}close(){this.connectionState='closed';}
    createDataChannel(){channel={readyState:'connecting',send:value=>sent.push(JSON.parse(value)),close(){this.readyState='closed';}};return channel;}
    async createOffer(){return {type:'offer',sdp:SDP};}async setLocalDescription(value){this.localDescription=value;}async setRemoteDescription(){this.connectionState='connected';channel.readyState='open';channel.onopen();}
  }
  class MeterContext{
    constructor(){this.state='running';}resume(){return Promise.resolve();}close(){this.state='closed';return Promise.resolve();}
    createMediaStreamSource(stream){return {connect(analyser){analyser.stream=stream;},disconnect(){}};}
    createAnalyser(){return {fftSize:512,getFloatTimeDomainData(samples){samples.fill(this.stream?.amplitude??.06);},disconnect(){}};}
  }
  const runtime={document,navigator:{mediaDevices:{getUserMedia:async()=>stream}},location:{origin:'https://preview.test'},RTCPeerConnection:Peer,AbortController,
    setTimeout(fn,ms){const id=++serial;timers.set(id,{fn,ms});return id;},clearTimeout:id=>timers.delete(id),addEventListener:(key,fn)=>listeners.set(key,fn),removeEventListener:key=>listeners.delete(key),
    fetch:async(url,init)=>{const request=JSON.parse(init.body);requests.push(request);return Response.json(request.action==='start'?{sdp:SDP,stopToken:'synthetic-stop-token',maxDurationMs:120000}:request.action==='capabilities'?{enabled:true}:{stopped:true});}};
  if(mediaClock)Object.assign(runtime,{AudioContext:MeterContext,requestAnimationFrame(fn){const id=++frameSerial;frames.set(id,fn);return id;},cancelAnimationFrame:id=>frames.delete(id)});
  const voice=voiceApi.create({runtime,greeting:false,onLevel:value=>levels.push(value),...(mediaClock?{onPlaybackBlocked(){}}:{}),onTranscript:value=>transcripts.push(value),onListeningTranscript:value=>listening.push(value),onPlaybackState:value=>playback.push(value),onSpeechStarted:value=>speech.push(value),onTool:async(args,context)=>{calls.push({args,context});return {verified:true};}});
  t.after(async()=>{await voice.dispose();assert.equal(timers.size,0);assert.equal(frames.size,0);});
  const emit=event=>channel?.onmessage?.({data:JSON.stringify(event)}),responses=()=>sent.filter(value=>value.type==='response.create');
  function begin(itemId='input-current'){
    emit({type:'input_audio_buffer.speech_started',item_id:itemId});emit({type:'input_audio_buffer.speech_stopped',item_id:itemId});emit({type:'input_audio_buffer.committed',item_id:itemId});
    return {itemId,turnVersion:speech.at(-1).turnVersion};
  }
  function bind(responseId='response-current',request=responses().at(-1)){emit({type:'response.created',response:{id:responseId,metadata:request?.response.metadata}});return responseId;}
  function frame(){const entry=frames.entries().next().value;assert.ok(entry,'a native analyser frame is scheduled');frames.delete(entry[0]);entry[1]();return levels.at(-1);}
  function attachRemote(amplitude=.1){const remote={amplitude,getAudioTracks:()=>[track],getTracks:()=>[track]};peer.ontrack({streams:[remote]});return remote;}
  return {voice,runtime,document,channel:()=>channel,peer:()=>peer,audio:()=>audios.at(-1),frames,levels,frame,attachRemote,sent,requests,transcripts,listening,playback,speech,calls,emit,begin,bind,responses};
}

test('generated text retains part identity and never pretends to be a playback event',async t=>{
  const f=fixture(t);await f.voice.start();const turn=f.begin();f.bind();
  f.emit({type:'response.output_audio_transcript.delta',response_id:'response-current',item_id:'assistant-part',content_index:1,output_index:0,delta:'A thoughtful gift. '});
  assert.deepEqual(f.transcripts.at(-1),{role:'assistant',delta:'A thoughtful gift. ',final:false,responseId:'response-current',itemId:'assistant-part',contentIndex:1,outputIndex:0,inputItemId:turn.itemId,turnVersion:turn.turnVersion,currentTurn:true});
  assert.equal(f.playback.length,0);assert.equal(f.voice.state,'thinking');
  f.emit({type:'output_audio_buffer.started',response_id:'response-current'});f.emit({type:'output_audio_buffer.started',response_id:'response-current'});
  assert.deepEqual(f.playback,[{playing:true,cleared:false,responseId:'response-current',itemId:'assistant-part',inputItemId:turn.itemId,turnVersion:turn.turnVersion,currentTurn:true}]);
  f.emit({type:'response.output_audio_transcript.done',response_id:'response-current',item_id:'assistant-part',content_index:1,output_index:0,transcript:'A thoughtful gift. What does she like?'});
  f.emit({type:'response.done',response:{id:'response-current',status:'completed'}});
  assert.equal(f.playback.length,1);assert.equal(f.voice.state,'speaking');
  f.emit({type:'output_audio_buffer.stopped',response_id:'response-current'});assert.equal(f.playback.at(-1).playing,false);assert.equal(f.playback.at(-1).cleared,false);assert.equal(f.voice.state,'listening');
});

test('native start can identify an output item before any transcript; absent timing fields stay absent',async t=>{
  const f=fixture(t);await f.voice.start();f.begin();f.bind();f.emit({type:'response.output_item.added',response_id:'response-current',item:{type:'message',role:'assistant',id:'assistant-known'}});
  f.emit({type:'output_audio_buffer.started',response_id:'response-current'});assert.equal(f.playback.at(-1).itemId,'assistant-known');assert.equal(Object.hasOwn(f.playback.at(-1),'audioStartMs'),false);
  f.emit({type:'response.audio_transcript.delta',response_id:'response-current',item_id:'assistant-known',delta:'Legacy transcript'});assert.equal(f.transcripts.at(-1).contentIndex,null);assert.equal(f.transcripts.at(-1).outputIndex,null);assert.equal(f.transcripts.at(-1).currentTurn,true);
});

test('late item identity cannot turn a duplicate native start into a new playback clock',async t=>{
  const f=fixture(t);await f.voice.start();f.begin();f.bind();f.emit({type:'output_audio_buffer.started',response_id:'response-current'});assert.equal(f.playback[0].itemId,'');
  f.emit({type:'response.output_audio_transcript.delta',response_id:'response-current',item_id:'assistant-later',content_index:0,output_index:0,delta:'Arriving after native audio start.'});f.emit({type:'output_audio_buffer.started',response_id:'response-current'});
  assert.equal(f.playback.length,1);assert.equal(f.transcripts.at(-1).itemId,'assistant-later');assert.equal(f.voice.state,'speaking');
});

test('input ASR presentation can follow the live current item without granting uncommitted action authority',async t=>{
  const f=fixture(t);await f.voice.start();f.emit({type:'input_audio_buffer.speech_started',item_id:'current-input'});
  const delta={type:'conversation.item.input_audio_transcription.delta',item_id:'current-input',event_id:'delta-one',delta:'I am unsure'};
  f.emit(delta);assert.equal(f.listening.length,1);assert.equal(f.transcripts.length,0);assert.equal(f.calls.length,0);assert.equal(f.responses().length,0);
  f.emit({type:'input_audio_buffer.speech_stopped',item_id:'current-input'});f.emit(delta);assert.equal(f.listening.length,1);
  f.emit({type:'input_audio_buffer.committed',item_id:'current-input'});f.emit(delta);f.emit(delta);
  assert.deepEqual(f.listening,[{delta:'I am unsure',final:false,itemId:'current-input',turnVersion:f.speech.at(-1).turnVersion,currentTurn:true}]);assert.equal(f.transcripts.length,0);
  const final={type:'conversation.item.input_audio_transcription.completed',item_id:'current-input',event_id:'final-one',transcript:'I am unsure about the length.'};f.emit(final);f.emit(final);
  assert.equal(f.listening.length,2);assert.equal(f.listening.at(-1).final,true);assert.equal(f.transcripts.at(-1).role,'user');assert.equal(f.transcripts.at(-1).currentTurn,true);
  assert.equal(f.calls.length,0);assert.equal(f.responses().length,1);assert.equal(f.requests.filter(value=>value.action==='start').length,1);
});

test('unknown, malformed, old-item and hidden input presentation cannot change the active listener',async t=>{
  const f=fixture(t);await f.voice.start();const old=f.begin('input-old');f.bind('response-old');f.begin('input-new');f.bind('response-new');
  for(const item_id of ['input-old','unknown','',{'bad':'identity'}]){
    f.emit({type:'conversation.item.input_audio_transcription.delta',item_id,delta:'celebrate!'});f.emit({type:'conversation.item.input_audio_transcription.completed',item_id,transcript:'celebrate!'});
  }
  assert.equal(f.listening.length,0);const prior=f.transcripts.find(value=>value.itemId==='input-old');assert.equal(prior.turnVersion,old.turnVersion);assert.equal(prior.currentTurn,false);
  f.document.hidden=true;f.emit({type:'conversation.item.input_audio_transcription.delta',item_id:'input-new',delta:'hidden'});f.emit({type:'conversation.item.input_audio_transcription.completed',item_id:'input-new',transcript:'hidden'});assert.equal(f.listening.length,0);
});

for(const boundary of ['interrupt','new speech','stop','hidden'])test(boundary+' clears native presentation and late events cannot revive it',async t=>{
  const f=fixture(t);await f.voice.start();f.begin();f.bind();f.emit({type:'output_audio_buffer.started',response_id:'response-current'});const oldHandler=f.channel().onmessage;
  if(boundary==='interrupt')f.voice.interrupt();else if(boundary==='new speech')f.emit({type:'input_audio_buffer.speech_started',item_id:'next-input'});else if(boundary==='hidden'){f.document.hidden=true;f.runtime.document.hidden=true;await f.voice.stop('hidden');}else await f.voice.stop();
  assert.equal(f.playback.at(-1).playing,false);assert.equal(f.playback.at(-1).cleared,true);const count=f.playback.length,transcriptCount=f.transcripts.length;
  for(const event of [{type:'output_audio_buffer.started',response_id:'response-current'},{type:'response.output_audio_transcript.done',response_id:'response-current',item_id:'late',transcript:'Celebrate!'}])oldHandler({data:JSON.stringify(event)});
  assert.equal(f.playback.length,count);assert.equal(f.transcripts.length,transcriptCount);assert.equal(f.voice.state,boundary==='interrupt'||boundary==='new speech'?'listening':'idle');
});

test('interrupt invalidates queued response presentation even before native audio starts',async t=>{
  const f=fixture(t);await f.voice.start();f.begin();f.bind();f.emit({type:'response.output_audio_transcript.delta',response_id:'response-current',item_id:'future-audio',delta:'Unplayed words'});f.voice.interrupt();
  assert.deepEqual(f.playback.map(value=>[value.responseId,value.playing,value.cleared]),[['response-current',false,true]]);assert.equal(f.responses().length,1);
});

test('a bounded audio continuation receives its own binding; an older stop cannot stop its output',async t=>{
  const f=fixture(t);await f.voice.start();const turn=f.begin();f.bind();f.emit({type:'output_audio_buffer.started',response_id:'response-current'});
  f.emit({type:'response.done',response:{id:'response-current',status:'incomplete',status_details:{reason:'max_output_tokens'}}});assert.equal(f.responses().length,1);
  f.emit({type:'output_audio_buffer.stopped',response_id:'response-current'});assert.equal(f.responses().length,2);assert.equal(f.responses().at(-1).response.tool_choice,'none');f.bind('response-tail');
  f.emit({type:'output_audio_buffer.started',response_id:'response-tail'});f.emit({type:'response.output_audio_transcript.delta',response_id:'response-tail',item_id:'tail-part',content_index:0,output_index:0,delta:'The ending.'});
  assert.equal(f.transcripts.at(-1).responseId,'response-tail');assert.equal(f.transcripts.at(-1).turnVersion,turn.turnVersion);const count=f.playback.length;
  f.emit({type:'output_audio_buffer.stopped',response_id:'response-current'});assert.equal(f.playback.length,count);assert.equal(f.voice.state,'speaking');f.emit({type:'response.done',response:{id:'response-tail',status:'completed'}});f.emit({type:'output_audio_buffer.stopped',response_id:'response-tail'});assert.equal(f.voice.state,'listening');assert.equal(f.responses().length,2);
});

test('callbacks captured by an old peer cannot contaminate an explicit replacement session',async t=>{
  const f=fixture(t);await f.voice.start();f.begin('old-input');f.bind('old-response');const handler=f.channel().onmessage;await f.voice.stop();await f.voice.start();f.begin('new-input');f.bind('new-response');const before={playback:f.playback.length,listening:f.listening.length,transcripts:f.transcripts.length};
  for(const event of [{type:'output_audio_buffer.started',response_id:'old-response'},{type:'conversation.item.input_audio_transcription.delta',item_id:'old-input',delta:'old listening'},{type:'response.output_audio_transcript.done',response_id:'old-response',item_id:'old-output',transcript:'old answer'}])handler({data:JSON.stringify(event)});
  assert.deepEqual({playback:f.playback.length,listening:f.listening.length,transcripts:f.transcripts.length},before);assert.equal(f.voice.state,'thinking');assert.equal(f.requests.filter(value=>value.action==='start').length,2);
});

test('native level metadata carries actual local media time only for bound output',async t=>{
  const f=fixture(t,{mediaClock:true});await f.voice.start();f.attachRemote();f.audio().currentTime=.75;
  assert.equal(Object.hasOwn(f.frame(),'outputTimeMs'),false,'a remote stream alone is not a response playback clock');
  const turn=f.begin();f.bind();f.emit({type:'output_audio_buffer.started',response_id:'response-current'});
  f.audio().currentTime=1.234;const first=f.frame();
  assert.equal(first.outputTimeMs,1234);assert.equal(first.outputClock,'local-media-currentTime');assert.equal(first.responseId,'response-current');assert.equal(first.turnVersion,turn.turnVersion);assert.equal(first.currentTurn,true);
  assert.ok(Math.abs(first.input-.24)<.000001);assert.ok(Math.abs(first.output-.4)<.000001);
  assert.equal(Object.hasOwn(first,'audioStartMs'),false);assert.equal(Object.hasOwn(first,'wordTimeMs'),false);
  f.audio().currentTime=1.584;assert.equal(f.frame().outputTimeMs,1584,'the actual 350ms media advance is preserved, not replaced with one RAF duration');
  assert.equal(f.frame().outputTimeMs,1584,'a coarse repeated media clock is truthful and never fabricated as advancement');
  assert.equal(f.requests.filter(value=>value.action==='start').length,1);assert.equal(f.calls.length,0);
});

test('invalid, throwing, backward and huge native clocks are omitted without poisoning the measured watermark',async t=>{
  const f=fixture(t,{mediaClock:true});await f.voice.start();f.attachRemote();f.begin();f.bind();f.emit({type:'output_audio_buffer.started',response_id:'response-current'});
  const audio=f.audio();audio.currentTime=.6;assert.equal(f.frame().outputTimeMs,600);
  for(const value of [NaN,Infinity,-Infinity,-1,Number.MAX_VALUE,601,'1',null,undefined,.5,.59]){
    audio.currentTime=value;const sample=f.frame();assert.equal(Object.hasOwn(sample,'outputTimeMs'),false,String(value));assert.equal(Object.hasOwn(sample,'outputClock'),false);assert.ok(sample.output>.39,'clock failure does not break measured RMS or native playback');
  }
  Object.defineProperty(audio,'currentTime',{configurable:true,get(){throw Error('synthetic private media failure');}});assert.equal(Object.hasOwn(f.frame(),'outputTimeMs'),false);
  Object.defineProperty(audio,'currentTime',{configurable:true,writable:true,value:.7});assert.equal(f.frame().outputTimeMs,700,'a valid clock resumes from its last measured watermark');assert.equal(f.voice.state,'speaking');
});

for(const boundary of ['paused','ended','hidden','disconnected','blocked'])test('native '+boundary+' media never leaks an output clock',async t=>{
  const f=fixture(t,{mediaClock:true});await f.voice.start();const peer=f.peer();f.attachRemote();f.begin();f.bind();f.emit({type:'output_audio_buffer.started',response_id:'response-current'});f.audio().currentTime=1;assert.equal(f.frame().outputTimeMs,1000);
  if(boundary==='paused')f.audio().paused=true;
  else if(boundary==='ended')f.audio().ended=true;
  else if(boundary==='hidden')f.document.hidden=true;
  else if(boundary==='disconnected'){f.peer().connectionState='disconnected';f.peer().onconnectionstatechange();}
  else {f.audio().play=()=>Promise.reject(Error('synthetic autoplay refusal'));assert.equal(await f.voice.resumeAudio(),false);assert.equal(f.voice.playbackBlocked,true);}
  f.audio().currentTime=1.3;const sample=f.frame();assert.equal(Object.hasOwn(sample,'outputTimeMs'),false);assert.equal(Object.hasOwn(sample,'responseId'),false);
  if(boundary==='paused')f.audio().paused=false;
  else if(boundary==='ended')f.audio().ended=false;
  else if(boundary==='hidden')f.document.hidden=false;
  else if(boundary==='disconnected'){f.peer().connectionState='connected';f.peer().onconnectionstatechange();}
  else {f.audio().play=function(){this.paused=false;return Promise.resolve();};assert.equal(await f.voice.resumeAudio(),true);}
  f.audio().currentTime=1.4;
  if(boundary==='disconnected'){
    const retired=f.frame();assert.equal(Object.hasOwn(retired,'outputTimeMs'),false);assert.equal(Object.hasOwn(retired,'responseId'),false);assert.equal(retired.output,0);assert.equal(f.voice.state,'listening');assert.equal(f.peer(),peer);
    f.emit({type:'output_audio_buffer.started',response_id:'response-current'});assert.equal(Object.hasOwn(f.frame(),'outputTimeMs'),false);
    const fresh=f.begin('input-recovered');f.bind('response-recovered');f.emit({type:'output_audio_buffer.started',response_id:'response-recovered'});f.audio().currentTime=1.6;const recovered=f.frame();assert.equal(recovered.outputTimeMs,1600);assert.equal(recovered.responseId,'response-recovered');assert.equal(recovered.turnVersion,fresh.turnVersion);assert.ok(recovered.output>.39);
    f.emit({type:'output_audio_buffer.stopped',response_id:'response-current'});assert.equal(f.frame().responseId,'response-recovered');assert.equal(f.requests.filter(value=>value.action==='stop').length,0);
  }else assert.equal(f.frame().outputTimeMs,1400);
  assert.equal(f.requests.filter(value=>value.action==='start').length,1,'clock recovery never opens another provider session');
});

test('native clear, turn replacement and disconnect remove old clock identity',async t=>{
  const f=fixture(t,{mediaClock:true});await f.voice.start();f.attachRemote();f.begin('input-old');f.bind('response-old');f.emit({type:'output_audio_buffer.started',response_id:'response-old'});f.audio().currentTime=1.7;assert.equal(f.frame().responseId,'response-old');
  f.emit({type:'output_audio_buffer.cleared',response_id:'response-old'});assert.equal(Object.hasOwn(f.frame(),'outputTimeMs'),false);
  f.begin('input-new');f.bind('response-new');f.emit({type:'output_audio_buffer.started',response_id:'response-new'});f.audio().currentTime=.2;const current=f.frame();assert.equal(current.responseId,'response-new');assert.equal(current.outputTimeMs,200,'a new response gets an independent observed media baseline');
  f.emit({type:'output_audio_buffer.started',response_id:'response-old'});assert.equal(f.frame().responseId,'response-new','late old response cannot claim the current media clock');
  f.voice.interrupt();assert.equal(Object.hasOwn(f.levels.at(-1),'outputTimeMs'),false);assert.equal(Object.hasOwn(f.frame(),'outputTimeMs'),false);
  const staleFrame=f.frames.values().next().value;await f.voice.stop();assert.deepEqual(f.levels.at(-1),{input:0,output:0,outputSignal:{amplitude:0,bands:[0,0,0,0,0,0],brightness:0,valid:false}});const count=f.levels.length;staleFrame();assert.equal(f.levels.length,count,'a frame captured before cleanup cannot publish a disconnected clock');assert.equal(f.frames.size,0);
});

test('remote stream replacement accepts its actual new local clock without retaining a previous stream watermark',async t=>{
  const f=fixture(t,{mediaClock:true});await f.voice.start();f.attachRemote();f.begin();f.bind();f.emit({type:'output_audio_buffer.started',response_id:'response-current'});f.audio().currentTime=2;assert.equal(f.frame().outputTimeMs,2000);
  f.attachRemote(.08);f.audio().currentTime=.03;const sample=f.frame();assert.equal(sample.outputTimeMs,30);assert.equal(sample.responseId,'response-current');assert.ok(Math.abs(sample.output-.32)<.000001);assert.equal(f.requests.filter(value=>value.action==='start').length,1);
});
