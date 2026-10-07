'use strict';
// Production voice code with synthetic transport and measured PCM/FFT samples.
// These tests do not access a microphone, invoke a provider, establish device
// audibility, certify human emotion or change any saved preview allocation.
const test=require('node:test'),assert=require('node:assert/strict');
const client=require('../../brites-concierge-voice.js');
const server=require('../../netlify/functions/_britesConciergeVoice.js');
const deadline=require('../../netlify/functions/_britesConciergeVoiceDeadline.js');
const SDP='v=0\r\nm=audio 9 UDP/TLS/RTP/SAVPF 111\r\na=candidate:1 1 UDP 123 192.0.2.10 49152 typ host\r\n';
const SILENT={amplitude:0,bands:[0,0,0,0,0,0],brightness:0,valid:false};
const QUIET={level:0,signal:SILENT,speaking:false,itemId:'',turnVersion:null,currentTurn:false};
const near=(a,b)=>assert.ok(Math.abs(a-b)<.000001,String(a)+' differs from '+b);
function fixture(t,{exhausted=false,spectrum=true}={}){
  let serial=0,frameSerial=0,micRequests=0,peer,channel;
  const requests=[],sent=[],input=[],levels=[],listening=[],transcripts=[],frames=new Map(),timers=new Map(),contexts=[],listeners=new Map(),audios=[];
  const track={readyState:'live',enabled:true,muted:false,stop(){}},microphone={amplitude:.06,bin:4,getTracks:()=>[track],getAudioTracks:()=>[track]};
  const document={hidden:false,body:{appendChild(){}},createElement(){const a={paused:true,ended:false,muted:false,volume:1,currentTime:0,setAttribute(){},play(){this.paused=false;return Promise.resolve();},pause(){this.paused=true;},remove(){}};audios.push(a);return a;},addEventListener:(key,fn)=>listeners.set(key,fn),removeEventListener:key=>listeners.delete(key)};
  class Peer{
    constructor(){peer=this;this.iceGatheringState='complete';this.connectionState='new';}addTrack(){}close(){this.connectionState='closed';}
    createDataChannel(){channel={readyState:'connecting',send:value=>sent.push(JSON.parse(value)),close(){this.readyState='closed';}};return channel;}
    async createOffer(){return {type:'offer',sdp:SDP};}async setLocalDescription(value){this.localDescription=value;}async setRemoteDescription(){this.connectionState='connected';channel.readyState='open';channel.onopen();}
  }
  class AudioContext{
    constructor(){this.state='running';this.sampleRate=48000;contexts.push(this);}resume(){return Promise.resolve();}close(){this.state='closed';return Promise.resolve();}
    createMediaStreamSource(stream){return {connect(analyser){analyser.stream=stream;},disconnect(){}};}
    createAnalyser(){const analyser={fftSize:512,smoothingTimeConstant:.8,get frequencyBinCount(){return this.fftSize/2;},getFloatTimeDomainData(samples){if(this.stream?.badWave)throw Error('synthetic bad waveform');samples.fill(this.stream?.amplitude??0);},disconnect(){}};
      if(spectrum)analyser.getFloatFrequencyData=function(samples){samples.fill(-Infinity);if(this.stream?.badSpectrum)return;samples[this.stream?.bin??4]=-20;};return analyser;}
  }
  const runtime={document,navigator:{mediaDevices:{getUserMedia:async()=>{micRequests++;return microphone;}}},location:{origin:'https://preview.test'},RTCPeerConnection:Peer,AudioContext,AbortController,
    setTimeout(fn,ms){const id=++serial;timers.set(id,{fn,ms});return id;},clearTimeout:id=>timers.delete(id),addEventListener:(key,fn)=>listeners.set(key,fn),removeEventListener:key=>listeners.delete(key),
    requestAnimationFrame(fn){const id=++frameSerial;frames.set(id,fn);return id;},cancelAnimationFrame:id=>frames.delete(id),
    fetch:async(url,init)=>{const body=JSON.parse(init.body);requests.push(body);if(body.action==='capabilities'&&exhausted)return Response.json({enabled:false,code:'VOICE_ALLOCATION_UNAVAILABLE'},{status:429});return Response.json(body.action==='capabilities'?{enabled:true}:body.action==='start'?{sdp:SDP,stopToken:'synthetic-stop',maxDurationMs:120000}:{stopped:true});}};
  const voice=client.create({runtime,greeting:false,onInputSignal:value=>input.push(value),onLevel:value=>levels.push(value),onListeningTranscript:value=>listening.push(value),onTranscript:value=>transcripts.push(value)});
  t.after(async()=>{await voice.dispose();assert.equal(frames.size,0);assert.equal(timers.size,0);});
  const emit=event=>channel.onmessage?.({data:JSON.stringify(event)}),responses=()=>sent.filter(row=>row.type==='response.create');
  function frame(){const entry=frames.entries().next().value;assert.ok(entry);frames.delete(entry[0]);entry[1]();return input.at(-1);}
  function speech(itemId='input-current'){emit({type:'input_audio_buffer.speech_started',item_id:itemId});return voice.currentInput;}
  function bindOutput(){emit({type:'input_audio_buffer.speech_stopped',item_id:'input-current'});emit({type:'input_audio_buffer.committed',item_id:'input-current'});const request=responses().at(-1);emit({type:'response.created',response:{id:'response-current',metadata:request.response.metadata}});emit({type:'output_audio_buffer.started',response_id:'response-current'});}
  function remote(){peer.ontrack({streams:[{amplitude:.1,bin:64,getTracks:()=>[track],getAudioTracks:()=>[track]}]});}
  return {voice,runtime,document,track,microphone,input,levels,listening,transcripts,requests,frames,contexts,sent,emit,frame,speech,bindOutput,remote,peer:()=>peer,audio:()=>audios.at(-1),responses,micRequests:()=>micRequests};
}

test('real incoming PCM and spectral bands drive a separately bound listening cue before any output response',async t=>{
  const f=fixture(t);await f.voice.start();assert.deepEqual(f.input.at(-1),QUIET);const binding=f.speech();assert.equal(binding.speaking,true);const value=f.frame();
  assert.equal(value.currentTurn,true);assert.equal(value.itemId,'input-current');assert.equal(value.turnVersion,binding.turnVersion);near(value.level,.24);near(value.signal.amplitude,.24);assert.equal(value.signal.valid,true);near(value.signal.bands[1],.24);assert.equal(value.signal.bands[5],0);
  assert.equal(f.levels.at(-1).output,0);assert.deepEqual(f.levels.at(-1).outputSignal,SILENT);assert.equal(f.voice.currentOutput,null);assert.equal(f.responses().length,0);assert.equal(f.listening.length,0);
  const snapshot=f.voice.currentInput;snapshot.itemId='forged';assert.equal(f.voice.currentInput.itemId,'input-current','returned native identity cannot mutate adapter authority');
});

test('input energy varies with current measured samples, not a simulated conversational waveform',async t=>{
  const f=fixture(t);await f.voice.start();f.speech();const first=f.frame();f.microphone.amplitude=.1;f.microphone.bin=64;const changed=f.frame();near(first.level,.24);near(changed.level,.4);assert.equal(changed.signal.bands[1],0);near(changed.signal.bands[5],.4);
  f.microphone.amplitude=0;const silence=f.frame();assert.equal(silence.level,0);assert.deepEqual(silence.signal,SILENT);assert.equal(silence.speaking,true);assert.equal(silence.currentTurn,true);
  f.emit({type:'input_audio_buffer.speech_stopped',item_id:'input-current'});assert.deepEqual(f.input.at(-1),QUIET);assert.equal(f.voice.currentInput,null);assert.deepEqual(f.frame(),QUIET);
});

for(const boundary of ['hidden','disconnected','muted','disabled','ended','suspended','interrupted-context','closed-context','bad-waveform'])test(boundary+' input clears measured presentation without fabricating a spectrum',async t=>{
  const f=fixture(t);await f.voice.start();f.speech();assert.equal(f.frame().signal.valid,true);
  if(boundary==='hidden')f.document.hidden=true;else if(boundary==='disconnected')f.peer().connectionState='disconnected';else if(boundary==='muted')f.track.muted=true;else if(boundary==='disabled')f.track.enabled=false;else if(boundary==='ended')f.track.readyState='ended';else if(boundary==='bad-waveform')f.microphone.badWave=true;else f.contexts[0].state=boundary==='closed-context'?'closed':boundary==='interrupted-context'?'interrupted':'suspended';
  const frame=f.frame();if(['suspended','interrupted-context','closed-context','bad-waveform'].includes(boundary)){assert.equal(frame.currentTurn,true);assert.equal(frame.itemId,'input-current');assert.equal(frame.speaking,true);assert.equal(frame.level,0);assert.deepEqual(frame.signal,SILENT);}else assert.deepEqual(frame,QUIET);
  assert.equal(f.voice.state,'listening');assert.equal(f.requests.filter(row=>row.action==='start').length,1);
});

for(const shape of ['missing','unwritten'])test(shape+' input FFT preserves measured RMS but never invents frequency bands',async t=>{
  const f=fixture(t,{spectrum:shape!=='missing'});await f.voice.start();f.speech();if(shape==='unwritten')f.microphone.badSpectrum=true;const frame=f.frame();near(frame.level,.24);assert.equal(frame.speaking,true);assert.deepEqual(frame.signal,SILENT);assert.equal(f.voice.state,'listening');
});

for(const boundary of ['stop','dispose','interrupt','new-speech'])test(boundary+' invalidates old input identity and clears its measured cue',async t=>{
  const f=fixture(t);await f.voice.start();const previous=f.speech('old-input');assert.equal(f.frame().signal.valid,true);
  if(boundary==='stop')await f.voice.stop();else if(boundary==='dispose')await f.voice.dispose();else if(boundary==='interrupt')f.voice.interrupt();else f.speech('new-input');
  assert.deepEqual(f.input.at(-1),QUIET);if(boundary==='new-speech'){assert.equal(f.voice.currentInput.itemId,'new-input');assert.ok(f.voice.currentInput.turnVersion>previous.turnVersion);assert.equal(f.frame().itemId,'new-input');}else assert.equal(f.voice.currentInput,null);
});

test('exact native current-item partial ASR can inform listening presentation while final and tool authority stay uncommitted',async t=>{
  const f=fixture(t);await f.voice.start();f.speech('current');const partial={type:'conversation.item.input_audio_transcription.delta',item_id:'current',event_id:'partial-one',delta:'I am unsure about this'};f.emit(partial);f.emit(partial);
  assert.equal(f.listening.length,1);assert.equal(f.listening[0].currentTurn,true);assert.equal(f.transcripts.length,0);assert.equal(f.responses().length,0);
  f.emit({type:'conversation.item.input_audio_transcription.completed',item_id:'current',transcript:'Show that piece'});assert.equal(f.listening.length,1);assert.equal(f.transcripts.at(-1).currentTurn,false);assert.equal(f.responses().length,0);
  f.emit({type:'input_audio_buffer.speech_stopped',item_id:'current'});f.emit({...partial,event_id:'between',delta:'Still uncommitted'});assert.equal(f.listening.length,1);
  f.emit({type:'input_audio_buffer.committed',item_id:'current'});f.emit({type:'conversation.item.input_audio_transcription.completed',item_id:'current',event_id:'final-one',transcript:'Show that piece'});assert.equal(f.listening.at(-1).final,true);assert.equal(f.transcripts.at(-1).currentTurn,true);assert.equal(f.responses().length,1);
});

test('unknown, old and hidden input partials cannot change live listening presentation',async t=>{
  const f=fixture(t);await f.voice.start();f.speech('old');f.speech('current');for(const item_id of ['old','unknown','',{},null])f.emit({type:'conversation.item.input_audio_transcription.delta',item_id,delta:'old words'});assert.equal(f.listening.length,0);f.document.hidden=true;f.emit({type:'conversation.item.input_audio_transcription.delta',item_id:'current',delta:'hidden words'});assert.equal(f.listening.length,0);
});

for(const boundary of ['muted','zero-volume'])test(boundary+' local output clears the mouth spectrum and local clock while keeping the same session',async t=>{
  const f=fixture(t);await f.voice.start();f.speech();f.remote();f.bindOutput();f.audio().currentTime=.2;f.frame();assert.equal(f.levels.at(-1).outputSignal.valid,true);
  if(boundary==='muted')f.audio().muted=true;else f.audio().volume=0;f.frame();assert.equal(f.levels.at(-1).output,0);assert.deepEqual(f.levels.at(-1).outputSignal,SILENT);assert.equal(Object.hasOwn(f.levels.at(-1),'outputTimeMs'),false);
  f.audio().muted=false;f.audio().volume=1;f.audio().currentTime=.5;f.frame();assert.equal(f.levels.at(-1).outputSignal.valid,true);assert.equal(f.levels.at(-1).outputTimeMs,500);assert.equal(f.requests.filter(row=>row.action==='start').length,1);
});

test('exhausted cumulative allowance is checked again after reload without any microphone, start, old-token replay or automatic paid retry',async t=>{
  const f=fixture(t,{exhausted:true});assert.equal(f.micRequests(),0);assert.equal(f.requests.length,0);assert.equal(await f.voice.start(),false);assert.equal(f.voice.lastError.code,'VOICE_ALLOCATION_UNAVAILABLE');assert.equal(f.micRequests(),0);assert.equal(f.requests.length,1);
  await f.voice.dispose();const replacement=client.create({runtime:f.runtime,greeting:false});t.after(()=>replacement.dispose());assert.equal(await replacement.start(),false);assert.equal(replacement.lastError.code,'VOICE_ALLOCATION_UNAVAILABLE');assert.equal(f.micRequests(),0);assert.deepEqual(f.requests.map(row=>row.action),['capabilities','capabilities']);
});

function closureFixture({status=200,failWrite=false}={}){
  const at=1000000,key='synthetic-key',rows=new Map([
    ['VoiceDeadlines/rtc_synthetic',{callId:'rtc_synthetic',expiresAt:at+120000,state:'pending',at}],
    ['VoiceUsage/preview-budget',{reservedCents:1000,spentCents:0,calls:10}],
    ['VoiceAllowances/native-sandbox',{allocatedCents:500,reservedCents:500,spentCents:0,calls:5}]
  ]),writes=[],fetches=[];
  const env={BRITES_GROWTH_SANDBOX:'1',BRITES_GROWTH_NAMESPACE:'Brites_Growth_Sandbox',BRITES_CONCIERGE_REALTIME_ENABLED:'1',OPENAI_API_KEY:key};
  const service={namespace:'Brites_Growth_Sandbox',col:name=>({doc(id){const path=name+'/'+id;return {async get(){return {exists:rows.has(path),data:()=>rows.get(path)};},async set(value,options){if(failWrite)throw Error('synthetic store unavailable');writes.push({path,value,options});rows.set(path,{...rows.get(path),...value});}};}})};
  const handler=server.createHandler({env,service,authorize:async()=>true,now:()=>at,fetch:async(url,init)=>{fetches.push({url,method:init?.method});return new Response(null,{status});}});
  const stopToken=server.stopToken('rtc_synthetic',at+120000,key),request=new Request('https://preview.test/api/concierge-voice',{method:'POST',headers:{Origin:'https://preview.test','Content-Type':'application/json'},body:JSON.stringify({action:'stop',stopToken})});
  return {handler,request,rows,writes,fetches};
}

for(const status of [200,404])test('server '+status+' exact explicit hangup records closure once and preserves all held allocation',async()=>{
  const f=closureFixture({status}),budget=JSON.stringify(f.rows.get('VoiceUsage/preview-budget')),allowance=JSON.stringify(f.rows.get('VoiceAllowances/native-sandbox')),response=await f.handler(f.request.clone());assert.equal((await response.json()).stopped,true);assert.equal(f.rows.get('VoiceDeadlines/rtc_synthetic').state,'closed');assert.equal(f.rows.get('VoiceDeadlines/rtc_synthetic').closeReason,'provider-hangup');assert.equal(f.rows.get('VoiceDeadlines/rtc_synthetic').lastStatus,status);
  assert.deepEqual(f.writes.map(row=>row.path),['VoiceDeadlines/rtc_synthetic']);assert.equal(JSON.stringify(f.rows.get('VoiceUsage/preview-budget')),budget);assert.equal(JSON.stringify(f.rows.get('VoiceAllowances/native-sandbox')),allowance);
  assert.equal((await (await f.handler(f.request.clone())).json()).stopped,true);assert.equal(f.writes.length,1,'repeated confirmed stop cannot overwrite completed closure or release usage');
});

test('provider503 explicit hangup remains pending and never pretends to settle billed usage',async()=>{
  const f=closureFixture({status:503}),before=JSON.stringify([...f.rows]),response=await f.handler(f.request);assert.equal((await response.json()).stopped,false);assert.equal(JSON.stringify([...f.rows]),before);assert.equal(f.writes.length,0);
});

test('closure-note storage failure cannot erase the backup deadline or deny an observed successful provider hangup',async()=>{
  const f=closureFixture({failWrite:true}),response=await f.handler(f.request);assert.equal((await response.json()).stopped,true);assert.equal(f.rows.get('VoiceDeadlines/rtc_synthetic').state,'pending');assert.equal(f.writes.length,0);
});

test('bounded reaper rotates503 expired calls so newer due calls are not permanently starved',async()=>{
  let at=1000000;const rows=new Map(Array.from({length:5},(_,n)=>['rtc_due'+n,{callId:'rtc_due'+n,state:'pending',expiresAt:at-1000,lastAttemptAt:0}])),calls=[];
  rows.set('rtc_future',{callId:'rtc_future',state:'pending',expiresAt:at+300000});
  const collection={doc:id=>({async get(){return {exists:rows.has(id),data:()=>rows.get(id)};},async set(value){rows.set(id,{...rows.get(id),...value});}}),where:()=>({limit:()=>({async get(){return {docs:[...rows].filter(([,row])=>row.state==='pending').map(([id,row])=>({id,data:()=>row}))};}})})};
  const f={env:{BRITES_CONCIERGE_REALTIME_ENABLED:'1',BRITES_GROWTH_NAMESPACE:'Brites_Growth_Sandbox',OPENAI_API_KEY:'synthetic-key'},service:{col:()=>collection},now:()=>at,fetch:async url=>{calls.push(url.split('/').at(-2));return {ok:false,status:503};}};
  assert.deepEqual(await deadline.reap(f),{closed:0});assert.equal(calls.length,4);assert.equal(calls.includes('rtc_due4'),false);at+=60000;assert.deepEqual(await deadline.reap(f),{closed:0});assert.equal(calls.length,8);assert.equal(calls.slice(4).includes('rtc_due4'),true);assert.equal(calls.includes('rtc_future'),false);assert.ok([...rows.values()].every(row=>row.state==='pending'));assert.ok([...rows.values()].every(row=>row.reservedCents===undefined));
});

test('unknown hangup transport failure is recorded as an attempt without creating closure or financial evidence',async()=>{
  const at=1000000,row={callId:'rtc_transport',state:'pending',expiresAt:at-1},writes=[];
  const service={col:()=>({doc:()=>({async get(){return {exists:true,data:()=>row};},async set(value){writes.push(value);Object.assign(row,value);}})})};
  const closed=await deadline.finish({env:{BRITES_CONCIERGE_REALTIME_ENABLED:'1',BRITES_GROWTH_NAMESPACE:'Brites_Growth_Sandbox',OPENAI_API_KEY:'synthetic-key'},service,callId:'rtc_transport',now:()=>at,fetch:async()=>{throw Error('synthetic private transport detail');}});
  assert.equal(closed,false);assert.equal(row.state,'pending');assert.equal(row.lastAttemptAt,at);assert.equal(row.lastStatus,null);assert.equal(row.closedAt,undefined);assert.deepEqual(writes,[{lastAttemptAt:at,lastStatus:null}]);
});
