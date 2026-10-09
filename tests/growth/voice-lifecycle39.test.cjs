'use strict';
// Actual native adapter with synthetic media/transport and a controllable clock.
// No microphone/provider request, paid call, physical playback or timing claim.
const test=require('node:test'),assert=require('node:assert/strict');
const native=require('../../brites-concierge-voice.js');
const SDP='v=0\r\nm=audio 9 UDP/TLS/RTP/SAVPF 111\r\na=candidate:offline 1 udp 123 192.0.2.1 49152 typ host\r\n';
async function settle(){for(let n=0;n<12;n++)await Promise.resolve();}
function fixture(t,{finalHook=false,hangStop=false,meter=false}={}){
  let now=0,serial=0,frameSerial=0,peer,channel,stops=0,hostCalls=0;
  const timers=new Map(),frames=new Map(),sent=[],requests=[],warnings=[],phases=[],transcripts=[],states=[],playback=[];
  const track={readyState:'live',stop(){stops++;}},microphone={amplitude:0,getTracks:()=>[track],getAudioTracks:()=>[track]},remote={amplitude:.08,getTracks:()=>[track]};
  const audio={paused:true,ended:false,muted:false,volume:1,currentTime:0,srcObject:null,setAttribute(){},play(){this.paused=false;return Promise.resolve();},pause(){this.paused=true;},remove(){}};
  class Peer{
    constructor(){peer=this;this.iceGatheringState='complete';this.connectionState='new';}
    addTrack(){}createDataChannel(){channel={readyState:'connecting',send(value){sent.push(JSON.parse(value));},close(){this.readyState='closed';}};return channel;}
    async createOffer(){return {sdp:SDP};}async setLocalDescription(value){this.localDescription=value;}
    async setRemoteDescription(){this.connectionState='connected';channel.readyState='open';channel.onopen();}
    close(){this.connectionState='closed';}
  }
  class AudioContext{
    constructor(){this.state='running';this.sampleRate=48000;}resume(){return Promise.resolve();}close(){this.state='closed';return Promise.resolve();}
    createMediaStreamSource(stream){return {connect(analyser){analyser.stream=stream;},disconnect(){}};}
    createAnalyser(){return {fftSize:512,smoothingTimeConstant:0,getFloatTimeDomainData(samples){samples.fill(this.stream?.amplitude||0);},disconnect(){}};}
  }
  const runtime={document:{hidden:false,body:{appendChild(){}},createElement:()=>audio,addEventListener(){},removeEventListener(){}},navigator:{mediaDevices:{getUserMedia:async()=>microphone}},location:{origin:'https://offline.test'},RTCPeerConnection:Peer,AbortController,
    ...(meter?{AudioContext}:{}),setTimeout(fn,ms){const id=++serial;timers.set(id,{at:now+ms,ms,fn});return id;},clearTimeout:id=>timers.delete(id),addEventListener(){},removeEventListener(){},
    requestAnimationFrame(fn){const id=++frameSerial;frames.set(id,fn);return id;},cancelAnimationFrame:id=>frames.delete(id),
    fetch:async(_url,init)=>{const body=JSON.parse(init.body);requests.push(body.action);if(body.action==='stop'&&hangStop)return new Promise(()=>{});return Response.json(body.action==='capabilities'?{enabled:true}:body.action==='start'?{sdp:SDP,stopToken:'synthetic-only',maxDurationMs:120000}:{stopped:true});}};
  const voice=native.create({runtime,greeting:false,onState:value=>states.push(value),onTurnWarning:value=>warnings.push(value),onConnectionPhase:value=>phases.push(value),onTranscript:value=>transcripts.push(value),onPlaybackState:value=>playback.push(value),
    ...(finalHook?{onFinalizedTurn:async()=>{hostCalls++;return {handled:true,ok:true,reply:'Local action completed.'};}}:{}),
    onTool:async()=>{hostCalls++;return {verified:true,reply:'Checked.',products:[]};}});
  t.after(async()=>{const ending=voice.dispose();if(hangStop)await advance(5500);await ending;assert.equal(timers.size,0);assert.equal(frames.size,0);});
  const emit=value=>channel.onmessage?.({data:JSON.stringify(value)}),responses=()=>sent.filter(value=>value.type==='response.create');
  async function advance(ms){const target=now+ms;while(true){const item=[...timers].filter(([,value])=>value.at<=target).sort((a,b)=>a[1].at-b[1].at)[0];if(!item)break;now=item[1].at;timers.delete(item[0]);item[1].fn();await settle();}now=target;await settle();}
  function speech(itemId='input-current',{commit=true,final=finalHook}={}){emit({type:'input_audio_buffer.speech_started',item_id:itemId});emit({type:'input_audio_buffer.speech_stopped',item_id:itemId});if(commit)emit({type:'input_audio_buffer.committed',item_id:itemId});if(final)emit({type:'conversation.item.input_audio_transcription.completed',item_id:itemId,transcript:'Open this piece'});}
  function bind(id='response-current',request=responses().at(-1)){assert.ok(request);emit({type:'response.created',response:{id,metadata:request.response.metadata}});return request;}
  function output(id='response-current'){emit({type:'output_audio_buffer.started',response_id:id});}
  function frame(){audio.currentTime=now/1000;const entry=frames.entries().next().value;assert.ok(entry);frames.delete(entry[0]);entry[1]();}
  function sameSession(){assert.equal(requests.filter(value=>value==='start').length,1);assert.equal(requests.filter(value=>value==='stop').length,0);assert.equal(peer.connectionState,'connected');}
  function onlySessionTimer(){assert.equal(timers.size,1);assert.equal([...timers.values()][0].ms,120000);}
  return {voice,emit,advance,speech,bind,output,frame,responses,sent,requests,warnings,phases,transcripts,states,playback,timers,frames,audio,remote,peer:()=>peer,stops:()=>stops,hostCalls:()=>hostCalls,sameSession,onlySessionTimer,attach:()=>peer.ontrack({streams:[remote]})};
}

test('missing VAD commit releases thinking and permanently expires delayed authority in the same session',async t=>{
  const f=fixture(t,{finalHook:true});await f.voice.start();f.speech('missing-commit',{commit:false,final:false});assert.equal(f.voice.state,'thinking');await f.advance(4999);assert.equal(f.voice.state,'thinking');await f.advance(1);
  assert.equal(f.voice.state,'listening');assert.equal(f.warnings.length,1);assert.equal(f.hostCalls(),0);assert.equal(f.responses().length,0);f.onlySessionTimer();f.sameSession();
  f.emit({type:'input_audio_buffer.speech_stopped',item_id:'missing-commit'});f.emit({type:'input_audio_buffer.committed',item_id:'missing-commit'});f.emit({type:'conversation.item.input_audio_transcription.completed',item_id:'missing-commit',transcript:'Open this piece'});await settle();assert.equal(f.voice.state,'listening');assert.equal(f.hostCalls(),0);assert.equal(f.responses().length,0);
});

test('missing finalized ASR releases its committed turn and cannot be revived by a late final',async t=>{
  const f=fixture(t,{finalHook:true});await f.voice.start();f.speech('missing-final',{final:false});await f.advance(5000);assert.equal(f.voice.state,'listening');f.onlySessionTimer();f.sameSession();
  f.emit({type:'conversation.item.input_audio_transcription.completed',item_id:'missing-final',transcript:'Open this piece'});await settle();assert.equal(f.hostCalls(),0);assert.equal(f.responses().length,0);
});

for(const created of [false,true])test('missing response '+(created?'completion':'creation')+' releases the turn after bounded inactivity and rejects late output/tools',async t=>{
  const f=fixture(t);await f.voice.start();f.speech();const issued=f.responses().at(-1);if(created)f.bind();await f.advance(15000);assert.equal(f.voice.state,'listening');f.onlySessionTimer();f.sameSession();
  f.bind('late-response',issued);f.output('late-response');f.emit({type:'response.output_audio_transcript.done',response_id:'late-response',item_id:'late-content',transcript:'Late words'});f.emit({type:'response.function_call_arguments.done',response_id:'late-response',call_id:'late-call',name:'control_storefront',arguments:'{"type":"back"}'});await settle();assert.equal(f.voice.state,'listening');assert.equal(f.hostCalls(),0);assert.equal(f.transcripts.filter(value=>value.role==='assistant').length,0);assert.equal(f.responses().length,1);
});

test('only the nested exact current client event ID settles a response error immediately',async t=>{
  const f=fixture(t);await f.voice.start();f.speech('old-input');const old=f.responses().at(-1);assert.equal(old.event_id,old.response.metadata.brites_voice_request);
  f.emit({type:'error',event_id:'server-notification',error:{event_id:old.event_id,type:'invalid_request_error',message:'PRIVATE PROVIDER DETAIL'}});assert.equal(f.voice.state,'listening');f.onlySessionTimer();assert.equal(f.warnings.length,1);assert.ok(f.sent.slice(-2).some(value=>value.type==='output_audio_buffer.clear'));assert.ok(!JSON.stringify(f.warnings).includes('PRIVATE'));
  f.speech('new-input');const fresh=f.bind('new-response');f.emit({type:'error',event_id:fresh.event_id,error:{event_id:old.event_id,type:'server_error'}});assert.equal(f.voice.state,'thinking');assert.equal(f.warnings.length,1);
  f.emit({type:'error',event_id:fresh.event_id,error:{event_id:'unknown-client-event',type:'server_error'}});assert.equal(f.voice.state,'thinking');assert.equal(f.warnings.length,1);
  f.emit({type:'response.done',response:{id:'new-response',status:'completed'}});assert.equal(f.voice.state,'listening');f.onlySessionTimer();f.sameSession();
});

test('unattributed recoverable errors do not guess which request failed and remain bounded by the response watchdog',async t=>{
  const f=fixture(t);await f.voice.start();f.speech();const issued=f.bind();f.emit({type:'error',event_id:issued.event_id,error:{type:'server_error'}});assert.equal(f.voice.state,'thinking');await f.advance(15000);assert.equal(f.voice.state,'listening');f.onlySessionTimer();f.sameSession();
});

test('retired commit and response timeout callbacks cannot cancel a newer valid turn',async t=>{
  const f=fixture(t);await f.voice.start();f.speech('old-uncommitted',{commit:false});const oldCommit=[...f.timers.values()].find(value=>value.ms===5000).fn;
  f.speech('old-response');const oldResponse=[...f.timers.values()].find(value=>value.ms===15000).fn;f.speech('current-response');f.bind('current');oldCommit();oldResponse();assert.equal(f.voice.state,'thinking');assert.equal(f.warnings.length,0);
  f.emit({type:'response.done',response:{id:'current',status:'completed'}});assert.equal(f.voice.state,'listening');f.onlySessionTimer();f.sameSession();
});

test('late old input stops and unqualified playback cannot replace a newer explicitly bound response',async t=>{
  const f=fixture(t);await f.voice.start();f.speech('old-input');f.bind('old-response');f.speech('current-input');f.bind('current-response');f.emit({type:'input_audio_buffer.speech_stopped',item_id:'old-input'});f.emit({type:'output_audio_buffer.started'});assert.equal(f.voice.state,'thinking');f.output('current-response');f.emit({type:'output_audio_buffer.stopped'});f.emit({type:'output_audio_buffer.cleared',response_id:'old-response'});assert.equal(f.voice.state,'speaking');
  f.emit({type:'response.done',response:{id:'current-response',status:'completed'}});f.emit({type:'output_audio_buffer.stopped',response_id:'current-response'});assert.equal(f.voice.state,'listening');f.onlySessionTimer();f.sameSession();
});

test('current native streaming progress extends response inactivity without a fixed total response cutoff',async t=>{
  const f=fixture(t);await f.voice.start();f.speech();f.bind();for(let n=0;n<6;n++){await f.advance(10000);f.emit({type:'response.output_audio_transcript.delta',response_id:'response-current',item_id:'content-current',delta:'More current words'});assert.equal(f.voice.state,'thinking');assert.equal(f.warnings.length,0);}
  f.emit({type:'response.done',response:{id:'response-current',status:'completed'}});assert.equal(f.voice.state,'listening');f.onlySessionTimer();f.sameSession();
});

test('missing audio drain releases speaking, clears native buffers and suppresses late old/idless playback',async t=>{
  const f=fixture(t);await f.voice.start();f.speech();f.bind();f.output();f.emit({type:'response.done',response:{id:'response-current',status:'completed'}});assert.equal(f.voice.state,'speaking');await f.advance(30000);assert.equal(f.voice.state,'listening');f.onlySessionTimer();f.sameSession();assert.equal(f.playback.at(-1).cleared,true);
  f.output();f.emit({type:'output_audio_buffer.started'});assert.equal(f.voice.state,'listening');assert.equal(f.responses().length,1);
});

test('healthy measured output can continue beyond the drain bound while silence with a missing drain expires',async t=>{
  const f=fixture(t,{meter:true});await f.voice.start();f.attach();f.speech();f.bind();f.output();f.emit({type:'response.done',response:{id:'response-current',status:'completed'}});
  for(let n=0;n<7;n++){await f.advance(10000);f.frame();assert.equal(f.voice.state,'speaking');assert.equal(f.warnings.length,0);}f.remote.amplitude=0;f.frame();await f.advance(29999);assert.equal(f.voice.state,'speaking');await f.advance(1);assert.equal(f.voice.state,'listening');f.onlySessionTimer();f.sameSession();
});

test('healthy streamed native output refreshes both waits without requiring the optional visual analyser',async t=>{
  const f=fixture(t);await f.voice.start();f.speech();f.bind();f.output();for(let n=0;n<6;n++){await f.advance(10000);f.emit({type:'response.output_audio_transcript.delta',response_id:'response-current',delta:'Current streamed speech'});assert.equal(f.voice.state,'speaking');assert.equal(f.warnings.length,0);}
  f.emit({type:'response.done',response:{id:'response-current',status:'completed'}});f.emit({type:'output_audio_buffer.stopped',response_id:'response-current'});assert.equal(f.voice.state,'listening');f.onlySessionTimer();f.sameSession();
});

test('normal generation completion retires only its response timer while playback drains independently',async t=>{
  const f=fixture(t);await f.voice.start();f.speech();f.bind();f.output();f.emit({type:'response.done',response:{id:'response-current',status:'completed'}});assert.equal(f.voice.state,'speaking');assert.equal([...f.timers.values()].some(value=>value.ms===15000),false);assert.equal([...f.timers.values()].some(value=>value.ms===30000),true);
  f.emit({type:'output_audio_buffer.stopped',response_id:'response-current'});assert.equal(f.voice.state,'listening');f.onlySessionTimer();await f.advance(30000);assert.equal(f.warnings.length,0);f.sameSession();
});

test('an older same-turn generation completion cannot retire the latest response request timer',async t=>{
  const f=fixture(t);await f.voice.start();f.speech();f.bind('first');f.emit({type:'response.function_call_arguments.done',response_id:'first',call_id:'inspect-once',name:'find_jewellery',arguments:'{"message":"A necklace"}'});await settle();assert.equal(f.responses().length,1);f.emit({type:'response.done',response:{id:'first',status:'completed'}});assert.equal(f.responses().length,2);f.bind('second');f.emit({type:'response.done',response:{id:'first',status:'completed'}});assert.equal(f.voice.state,'thinking');assert.equal([...f.timers.values()].some(value=>value.ms===15000),true);
  f.emit({type:'response.done',response:{id:'second',status:'completed'}});assert.equal(f.voice.state,'listening');f.onlySessionTimer();
});

test('disconnect retires current lifecycle waits and reconnect cannot revive the expired turn or open another session',async t=>{
  const f=fixture(t);await f.voice.start();f.speech();const issued=f.bind();f.output();const stale=[...f.timers.values()].filter(value=>[15000,30000].includes(value.ms)).map(value=>value.fn);f.peer().connectionState='disconnected';f.peer().onconnectionstatechange();assert.equal(f.voice.state,'listening');assert.equal([...f.timers.values()].some(value=>[15000,30000].includes(value.ms)),false);await f.advance(3000);f.peer().connectionState='connected';f.peer().onconnectionstatechange();f.onlySessionTimer();
  stale.forEach(fn=>fn());f.bind('old-after-reconnect',issued);f.output('old-after-reconnect');assert.equal(f.voice.state,'listening');assert.equal(f.warnings.length,0);f.sameSession();f.speech('fresh-after-reconnect');f.bind('fresh');f.emit({type:'response.done',response:{id:'fresh',status:'completed'}});f.onlySessionTimer();
});

test('failed recovery stops local media at its peer deadline and server-confirmation timeout returns idle without retry',async t=>{
  const f=fixture(t,{hangStop:true});await f.voice.start();f.speech();f.peer().connectionState='disconnected';f.peer().onconnectionstatechange();await f.advance(8000);assert.equal(f.voice.state,'closing');assert.ok(f.stops()>0);await f.advance(5500);assert.equal(f.voice.state,'idle');assert.equal(f.timers.size,0);assert.equal(f.requests.filter(value=>value==='start').length,1);
});

test('End and restart retire old deadlines and old session callbacks cannot stop the new session',async t=>{
  const f=fixture(t);await f.voice.start();f.speech();f.bind();f.output();const old=[...f.timers.values()].map(value=>value.fn);await f.voice.stop();assert.equal(f.timers.size,0);await f.voice.start();old.forEach(fn=>fn());assert.equal(f.voice.state,'listening');assert.equal(f.warnings.length,0);f.onlySessionTimer();assert.equal(f.requests.filter(value=>value==='start').length,2);
});
