'use strict';
// Deterministic transport/device failures against the real adapter. These
// doubles do not certify a physical microphone, browser ICE or audible speech.
const test=require('node:test'),assert=require('node:assert/strict');
const client=require('../../brites-concierge-voice.js');
const BASIC='v=0\r\nm=audio 9 UDP/TLS/RTP/SAVPF 111\r\na=mid:0\r\n';
const CANDIDATE='a=candidate:123 1 udp 2122260223 192.0.2.1 54321 typ host\r\n';
const tick=()=>new Promise(resolve=>setImmediate(resolve));
function fixture(t,{candidate=CANDIDATE,iceGathering='complete',microphone='live',meterState=null,greeting=false}={}){
  let at=0,serial=0,peer,channel,microphones=0,resumeTo=meterState,remote=null;
  const requests=[],errors=[],states=[],phases=[],tracks=[],audios=[],timers=new Map(),peers=[],levels=[],meterStates=[],contexts=[],connections=[],operations=[],frames=new Map(),sent=[];
  function track(){const listeners=new Map(),value={kind:'audio',readyState:microphone==='ended'?'ended':'live',stops:0,stop(){this.stops++;this.readyState='ended';},addEventListener(name,fn){listeners.set(name,fn);},removeEventListener(name,fn){if(listeners.get(name)===fn)listeners.delete(name);},end(){this.readyState='ended';listeners.get('ended')?.();},listeners};tracks.push(value);return value;}
  const stream=()=>{const values=microphone==='empty'?[]:[track()];return {amplitude:.2,getTracks:()=>values,getAudioTracks:()=>values};};
  class Peer{
    constructor(){peer=this;peers.push(this);this.connectionState='new';this.iceGatheringState=iceGathering;this.listeners=new Map();}
    addEventListener(name,fn){this.listeners.set(name,fn);}removeEventListener(name,fn){if(this.listeners.get(name)===fn)this.listeners.delete(name);}
    finishIce(){this.iceGatheringState='complete';this.listeners.get('icegatheringstatechange')?.();}
    addTrack(){}createDataChannel(){channel={readyState:'connecting',send(raw){sent.push(JSON.parse(raw));},close(){this.readyState='closed';}};return channel;}
    async createOffer(){return {type:'offer',sdp:BASIC+candidate};}async setLocalDescription(value){this.localDescription=value;}
    async setRemoteDescription(){this.connectionState='connected';channel.readyState='open';channel.onopen?.();}
    change(state){this.connectionState=state;this.onconnectionstatechange?.();}close(){this.closed=true;this.connectionState='closed';}
  }
  class AudioContext{
    constructor(){contexts.push(this);this.state=meterState;this.destination={speaker:true};operations.push('audio-context');}
    resume(){operations.push('resume-meter');if(resumeTo!=='hold'){this.state=resumeTo;this.onstatechange?.();}return Promise.resolve();}
    close(){this.state='closed';this.closed=true;return Promise.resolve();}
    createMediaStreamSource(value){return {connect(target){target.stream=value;connections.push({source:value,target});},disconnect(){}};}
    createAnalyser(){return {fftSize:512,stream:null,disconnect(){},getFloatTimeDomainData(values){values.fill(this.stream.amplitude);}};}
    change(value){this.state=value;this.onstatechange?.();}
  }
  class MediaStream{constructor(values){this.amplitude=values[0].amplitude;this.getAudioTracks=this.getTracks=()=>values;}}
  const doc={hidden:false,body:{appendChild(){}},createElement(){const audio={setAttribute(){},srcObject:null,play:async()=>{},pause(){this.paused=true;},remove(){this.removed=true;}};audios.push(audio);return audio;},addEventListener(){},removeEventListener(){}};
  const runtime={document:doc,location:{origin:'https://preview.test'},navigator:{mediaDevices:{getUserMedia:async()=>{microphones++;operations.push('microphone');return stream();}}},RTCPeerConnection:Peer,MediaStream,...(meterState?{AudioContext}:{}),AbortController,
    setTimeout(fn,ms){const id=++serial;timers.set(id,{fn,at:at+ms});return id;},clearTimeout(id){timers.delete(id);},addEventListener(){},removeEventListener(){},
    requestAnimationFrame(fn){const id=++serial;frames.set(id,fn);return id;},cancelAnimationFrame(id){frames.delete(id);},
    fetch:async(url,init)=>{const body=JSON.parse(init.body);requests.push(body);operations.push(body.action);return Response.json(body.action==='capabilities'?{enabled:true,demoToken:'synthetic-demo-token'}:body.action==='start'?{sdp:BASIC+CANDIDATE,stopToken:'synthetic-exact-stop-token',maxDurationMs:120000}:{stopped:true});}};
  const voice=client.create({runtime,greeting,onError:(message,detail)=>errors.push({message,detail}),onState:value=>states.push(value),onConnectionPhase:value=>phases.push(value),onLevel:value=>levels.push(value),onOutputMeterState:value=>meterStates.push(value)});
  t.after(()=>voice.dispose());
  return {voice,runtime,requests,errors,states,phases,tracks,audios,timers,peers,levels,meterStates,contexts,connections,operations,frames,sent,peer:()=>peer,channel:()=>channel,microphones:()=>microphones,
    setResumeTo:value=>{resumeTo=value;},remote:()=>remote,track({noStreams=false,amplitude=.05}={}){const value={kind:'audio',readyState:'live',amplitude},values=[value];remote={amplitude,getTracks:()=>values,getAudioTracks:()=>values};peer.ontrack({track:value,streams:noStreams?[]:[remote]});if(noStreams)remote=audios[0].srcObject;},
    emit:value=>channel.onmessage({data:JSON.stringify(value)}),frame(){for(const [id,fn]of [...frames]){frames.delete(id);fn();}},
    async advance(ms){at+=ms;for(const [id,value]of [...timers])if(value.at<=at){timers.delete(id);value.fn();}await tick();}};
}
for(const iceGathering of ['complete','gathering'])test('a '+iceGathering+' zero-candidate offer stops before a paid start',async t=>{
  const f=fixture(t,{candidate:'a=end-of-candidates\r\n',iceGathering}),starting=f.voice.start();await tick();
  if(iceGathering==='gathering'){assert.equal(f.requests.some(x=>x.action==='start'),false);f.peer().finishIce();}
  assert.equal(await starting,false);assert.deepEqual(f.requests.map(x=>x.action),['capabilities']);assert.equal(f.voice.state,'idle');assert.equal(f.voice.lastError.code,'VOICE_NETWORK_UNAVAILABLE');assert.match(f.voice.lastError.message,/Try another browser/);
  assert.ok(f.tracks[0].stops>0);assert.equal(f.peer().closed,true);assert.equal(f.channel().readyState,'closed');assert.equal(f.peer().listeners.size,0);assert.equal(f.timers.size,0);assert.equal(f.microphones(),1);
});
for(const candidate of ['a=candidate:bad 0 udp 1 192.0.2.1 4000 typ host\r\n','a=candidate:bad 1 udp 1 192.0.2.1 0 typ host\r\n','a=candidate:bad 1 udp 1 192.0.2.1 70000 typ host\r\n','a=candidate:bad 1 unknown 1 192.0.2.1 4000 typ host\r\n'])test('an unusable candidate cannot authorize network startup: '+candidate.trim(),async t=>{
  const f=fixture(t,{candidate});assert.equal(await f.voice.start(),false);assert.equal(f.requests.filter(x=>x.action==='start').length,0);assert.equal(f.voice.lastError.code,'VOICE_NETWORK_UNAVAILABLE');assert.equal(f.timers.size,0);
});
for(const candidate of [CANDIDATE,'a=candidate:456 1 UDP 123 192.0.2.5 49152 typ srflx raddr 192.0.2.1 rport 54321\r\n','a=candidate:789 1 udp 123 2001:db8::1 49152 typ relay\r\n','a=candidate:abc 1 udp 123 private-name.local 49152 typ host\r\n','a=candidate:def 1 tcp 123 192.0.2.1 9 typ host tcptype active\r\n'])test('a gathered standard candidate reaches exactly one provider start: '+candidate.trim(),async t=>{
  const f=fixture(t,{candidate});assert.equal(await f.voice.start(),true);assert.equal(f.requests.filter(x=>x.action==='start').length,1);assert.equal(f.voice.state,'listening');assert.equal(f.errors.length,0);
});
for(const microphone of ['empty','ended'])test('an '+microphone+' microphone stream cannot open a provider call',async t=>{
  const f=fixture(t,{microphone});assert.equal(await f.voice.start(),false);assert.equal(f.voice.lastError.code,'MIC_NOT_FOUND');assert.deepEqual(f.requests.map(x=>x.action),['capabilities']);assert.equal(f.voice.state,'idle');assert.equal(f.peers.length,0);assert.equal(f.timers.size,0);assert.equal(f.audios.length,0);
});
test('losing the current microphone ends local media and its exact provider call without a retry',async t=>{
  const f=fixture(t);await f.voice.start();f.tracks[0].end();await tick();assert.equal(f.voice.state,'idle');assert.equal(f.voice.lastError.code,'MIC_NOT_FOUND');assert.equal(f.peer().closed,true);assert.equal(f.audios[0].paused,true);assert.equal(f.tracks[0].listeners.size,0);
  assert.deepEqual(f.requests.at(-1),{action:'stop',stopToken:'synthetic-exact-stop-token'});assert.equal(f.requests.filter(x=>x.action==='start').length,1);assert.equal(f.microphones(),1);assert.equal(f.timers.size,0);
});
for(const boundary of ['stop','restart','dispose'])test('an obsolete microphone event after '+boundary+' cannot affect another session',async t=>{
  const f=fixture(t);await f.voice.start();const old=f.tracks[0],callback=old.listeners.get('ended');assert.equal(typeof callback,'function');await f.voice.stop();
  if(boundary==='restart')await f.voice.start();else if(boundary==='dispose')await f.voice.dispose();callback();await tick();assert.equal(f.errors.length,0);assert.equal(f.voice.state,boundary==='restart'?'listening':'idle');assert.equal(f.requests.filter(x=>x.action==='start').length,boundary==='restart'?2:1);assert.equal(old.listeners.size,0);
});
test('a sustained peer disconnection closes media instead of leaving a silent listening session',async t=>{
  const f=fixture(t);await f.voice.start();f.peer().change('disconnected');await f.advance(7999);assert.equal(f.voice.state,'listening');assert.equal(f.requests.filter(x=>x.action==='stop').length,0);await f.advance(1);
  assert.equal(f.voice.state,'idle');assert.equal(f.voice.lastError.code,'VOICE_MEDIA_FAILED');assert.equal(f.peer().closed,true);assert.ok(f.tracks[0].stops>0);assert.deepEqual(f.requests.at(-1),{action:'stop',stopToken:'synthetic-exact-stop-token'});assert.equal(f.requests.filter(x=>x.action==='start').length,1);assert.equal(f.timers.size,0);
});
test('a short network interruption can recover the existing call without paid reconnection',async t=>{
  const f=fixture(t);await f.voice.start();f.peer().change('disconnected');await f.advance(7000);f.peer().change('connected');await f.advance(1500);assert.equal(f.voice.state,'listening');assert.equal(f.errors.length,0);assert.equal(f.requests.filter(x=>x.action==='start').length,1);assert.equal(f.requests.filter(x=>x.action==='stop').length,0);assert.equal(f.microphones(),1);
});
test('repeat disconnected events cannot keep postponing the same recovery deadline',async t=>{
  const f=fixture(t);await f.voice.start();f.peer().change('disconnected');await f.advance(5000);f.peer().change('disconnected');await f.advance(3000);assert.equal(f.voice.state,'idle');assert.equal(f.requests.filter(x=>x.action==='stop').length,1);assert.equal(f.requests.filter(x=>x.action==='start').length,1);
});
test('an intermediate connecting state does not cancel the existing network recovery deadline',async t=>{
  const f=fixture(t);await f.voice.start();f.peer().change('disconnected');await f.advance(3000);f.peer().change('connecting');await f.advance(5000);assert.equal(f.voice.state,'idle');assert.equal(f.voice.lastError.code,'VOICE_MEDIA_FAILED');assert.equal(f.requests.filter(x=>x.action==='stop').length,1);assert.equal(f.requests.filter(x=>x.action==='start').length,1);
});
test('End voice cancels network recovery and stale events cannot stop an explicit new session',async t=>{
  const f=fixture(t);await f.voice.start();const old=f.peer(),callback=old.onconnectionstatechange;old.change('disconnected');await f.voice.stop();assert.equal(f.timers.size,0);await f.voice.start();callback();await f.advance(8000);assert.equal(f.voice.state,'listening');assert.equal(f.errors.length,0);assert.equal(f.requests.filter(x=>x.action==='start').length,2);assert.equal(f.requests.filter(x=>x.action==='stop').length,1);
});
test('optional audio analysis activation starts before network awaits but never acquires a microphone first',async t=>{
  const f=fixture(t,{meterState:'running'});assert.deepEqual(f.operations,[]);const starting=f.voice.start();assert.deepEqual(f.operations.slice(0,3),['audio-context','resume-meter','capabilities']);assert.equal(f.microphones(),0);assert.equal(await starting,true);assert.ok(f.operations.indexOf('microphone')>f.operations.indexOf('capabilities'));
});
test('remote RMS stays distinct from microphone energy and native buffer events define the talking interval',async t=>{
  const f=fixture(t,{meterState:'running',greeting:true});await f.voice.start();f.track();const request=f.sent.find(x=>x.type==='response.create');f.emit({type:'response.created',response:{id:'measured-response',metadata:request.response.metadata}});f.emit({type:'output_audio_buffer.started',response_id:'measured-response'});f.frame();
  assert.equal(f.voice.state,'speaking');assert.equal(f.voice.outputMeterState,'ready');assert.deepEqual(f.meterStates,['ready']);assert.ok(Math.abs(f.levels.at(-1).input-.8)<.00001);assert.ok(Math.abs(f.levels.at(-1).output-.2)<.00001);
  f.remote().amplitude=.15;f.frame();assert.ok(Math.abs(f.levels.at(-1).output-.6)<.00001);f.emit({type:'response.done',response:{id:'measured-response',status:'completed'}});assert.equal(f.voice.state,'speaking');f.remote().amplitude=0;f.frame();assert.equal(f.levels.at(-1).output,0);assert.ok(f.levels.at(-1).input>.79);
  f.emit({type:'output_audio_buffer.stopped',response_id:'measured-response'});assert.equal(f.voice.state,'listening');assert.equal(f.connections.length,2);assert.ok(f.connections.every(value=>value.target!==f.contexts[0].destination));await f.voice.stop();assert.deepEqual(f.levels.at(-1),{input:0,output:0});assert.equal(f.frames.size,0);
});
test('a suspended remote meter stays explicitly paused, reports zero energy, and resumes the same call',async t=>{
  const f=fixture(t,{meterState:'suspended'});f.setResumeTo('hold');await f.voice.start();f.track();f.frame();assert.equal(f.voice.outputMeterState,'paused');assert.deepEqual(f.meterStates,['paused']);assert.deepEqual(f.levels.at(-1),{input:0,output:0});assert.equal(f.voice.playbackBlocked,false);assert.equal(f.voice.state,'listening');
  f.setResumeTo('running');assert.equal(await f.voice.resumeAudio(),true);f.frame();assert.equal(f.voice.outputMeterState,'ready');assert.deepEqual(f.meterStates,['paused','ready']);assert.ok(f.levels.at(-1).output>.19);assert.equal(f.requests.filter(x=>x.action==='start').length,1);assert.equal(f.microphones(),1);assert.equal(f.errors.length,0);
});
test('audio playback success alone cannot certify that a suspended meter resumed',async t=>{
  const f=fixture(t,{meterState:'suspended'});f.setResumeTo('hold');await f.voice.start();f.track();assert.equal(await f.voice.resumeAudio(),true);assert.equal(f.voice.outputMeterState,'paused');assert.equal(f.voice.playbackBlocked,false);assert.deepEqual(f.meterStates,['paused']);assert.equal(f.requests.filter(x=>x.action==='start').length,1);
});
test('native speech continues when output analysis is unavailable, without fabricated energy',async t=>{
  const f=fixture(t);await f.voice.start();f.track();assert.equal(f.voice.outputMeterState,'unavailable');assert.deepEqual(f.meterStates,['unavailable']);assert.equal(f.voice.state,'listening');assert.equal(f.errors.length,0);assert.equal(f.contexts.length,0);assert.equal(await f.voice.resumeAudio(),true);assert.equal(f.voice.outputMeterState,'unavailable');assert.equal(f.requests.filter(x=>x.action==='start').length,1);
});
test('a streamless native audio track still gets its own playback and measured output stream',async t=>{
  const f=fixture(t,{meterState:'running'});await f.voice.start();f.track({noStreams:true,amplitude:.1});f.frame();assert.equal(f.voice.outputMeterState,'ready');assert.ok(Math.abs(f.levels.at(-1).output-.4)<.00001);assert.equal(f.audios[0].srcObject.getAudioTracks().length,1);assert.equal(f.errors.length,0);
});
test('context suspension mid-speech stops stale measured energy without stopping native audio',async t=>{
  const f=fixture(t,{meterState:'running'});await f.voice.start();f.track();f.frame();assert.ok(f.levels.at(-1).output>.19);f.contexts[0].change('interrupted');f.frame();assert.equal(f.voice.outputMeterState,'paused');assert.deepEqual(f.levels.at(-1),{input:0,output:0});assert.equal(f.voice.state,'listening');assert.equal(f.requests.filter(x=>x.action==='stop').length,0);f.contexts[0].change('running');f.frame();assert.equal(f.voice.outputMeterState,'ready');assert.ok(f.levels.at(-1).output>.19);
});
test('a failed remote analyser reports unavailable feedback while native media remains connected',async t=>{
  const f=fixture(t,{meterState:'running'});await f.voice.start();f.track();f.connections[1].target.getFloatTimeDomainData=()=>{throw Error('Private audio device detail');};f.frame();assert.equal(f.voice.outputMeterState,'unavailable');assert.deepEqual(f.meterStates,['ready','unavailable']);assert.equal(f.levels.at(-1).output,0);assert.equal(f.voice.state,'listening');assert.equal(f.errors.length,0);assert.equal(f.requests.filter(x=>x.action==='stop').length,0);assert.equal(f.requests.filter(x=>x.action==='start').length,1);
});
