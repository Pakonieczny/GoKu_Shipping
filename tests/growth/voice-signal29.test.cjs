'use strict';
// The production adapter and shared analyser helper use synthetic browser
// transport/device samples here. These checks establish real-sample admission,
// finite spectral normalization and cancellation, not physical audible sync.
const test=require('node:test'),assert=require('node:assert/strict');
const voiceApi=require('../../brites-concierge-voice.js');
const SDP='v=0\r\nm=audio 9 UDP/TLS/RTP/SAVPF 111\r\na=candidate:1 1 UDP 2122260223 192.0.2.10 50000 typ host\r\n';
const NO_SIGNAL={amplitude:0,bands:[0,0,0,0,0,0],brightness:0,valid:false};
const NO_LEVELS={input:0,output:0,outputSignal:NO_SIGNAL};
const tick=()=>new Promise(resolve=>setImmediate(resolve));
const near=(actual,expected)=>assert.ok(Math.abs(actual-expected)<.000001,actual+' differs from '+expected);
function valid(value){assert.equal(value.valid,true);for(const number of [value.amplitude,value.brightness,...value.bands])assert.ok(Number.isFinite(number)&&number>=0&&number<=1);assert.equal(value.bands.length,6);}
function direct({amplitude=.1,bins=[[4,-20]],fftSize=512,sampleRate=48000,read}={}){
  let reads=0;const analyser={fftSize,get frequencyBinCount(){return this.fftSize/2;},getFloatFrequencyData(values){reads++;if(read)return read(values);values.fill(-Infinity);for(const [index,db]of bins)values[index]=db;}};
  const waveform=new Float32Array(fftSize).fill(amplitude),frequencies=new Float32Array(fftSize/2).fill(-20);
  return {analyser,waveform,frequencies,signal:()=>voiceApi.measureOutputSignal(analyser,waveform,frequencies,sampleRate),reads:()=>reads};
}
function fixture(t,{sampleRate=48000,spectrum=true,meterState='running',greeting=false,onTool=async()=>({verified:true})}={}){
  let peer,channel,serial=0,frameSerial=0;const sent=[],requests=[],levels=[],states=[],playback=[],transcripts=[],listening=[],tools=[],audios=[],contexts=[],analysers=[],timers=new Map(),listeners=new Map(),frames=new Map();
  const microphone={amplitude:.2,bins:[[2,-20]],getAudioTracks:()=>[track],getTracks:()=>[track]},track={kind:'audio',readyState:'live',stop(){}};
  const document={hidden:false,body:{appendChild(){}},createElement(){const value={currentTime:0,paused:true,ended:false,srcObject:null,setAttribute(){},play(){this.paused=false;return Promise.resolve();},pause(){this.paused=true;},remove(){}};audios.push(value);return value;},addEventListener:(key,fn)=>listeners.set(key,fn),removeEventListener:key=>listeners.delete(key)};
  class Peer{
    constructor(){peer=this;this.connectionState='new';this.iceGatheringState='complete';}addTrack(){}close(){this.connectionState='closed';}
    createDataChannel(){channel={readyState:'connecting',send:value=>sent.push(JSON.parse(value)),close(){this.readyState='closed';}};return channel;}
    async createOffer(){return {type:'offer',sdp:SDP};}async setLocalDescription(value){this.localDescription=value;}async setRemoteDescription(){this.connectionState='connected';channel.readyState='open';channel.onopen();}
  }
  class MeterContext{
    constructor(){this.state=meterState;this.sampleRate=sampleRate;contexts.push(this);}resume(){return Promise.resolve();}close(){this.state='closed';return Promise.resolve();}
    createMediaStreamSource(stream){return {connect(analyser){analyser.stream=stream;},disconnect(){}};}
    createAnalyser(){const analyser={fftSize:2048,smoothingTimeConstant:.8,get frequencyBinCount(){return this.fftSize/2;},getFloatTimeDomainData(values){values.fill(this.stream?.amplitude??0);},disconnect(){}};
      if(spectrum)analyser.getFloatFrequencyData=function(values){values.fill(-Infinity);for(const [index,db]of this.stream?.bins||[])values[index]=db;};analysers.push(analyser);return analyser;}
  }
  const runtime={document,navigator:{mediaDevices:{getUserMedia:async()=>microphone}},location:{origin:'https://preview.test'},RTCPeerConnection:Peer,AudioContext:MeterContext,AbortController,
    setTimeout(fn,ms){const id=++serial;timers.set(id,{fn,ms});return id;},clearTimeout:id=>timers.delete(id),addEventListener:(key,fn)=>listeners.set(key,fn),removeEventListener:key=>listeners.delete(key),
    requestAnimationFrame(fn){const id=++frameSerial;frames.set(id,fn);return id;},cancelAnimationFrame:id=>frames.delete(id),
    fetch:async(url,init)=>{const request=JSON.parse(init.body);requests.push(request);return Response.json(request.action==='start'?{sdp:SDP,stopToken:'synthetic-stop-token',maxDurationMs:120000}:request.action==='capabilities'?{enabled:true}:{stopped:true});}};
  const voice=voiceApi.create({runtime,greeting,onLevel:value=>levels.push(value),onState:value=>states.push(value),onPlaybackBlocked(){},onTranscript:value=>transcripts.push(value),onListeningTranscript:value=>listening.push(value),onPlaybackState:value=>playback.push(value),onTool:async(args,context)=>{tools.push({args,context});return onTool(args,context);}});
  t.after(async()=>{await voice.dispose();assert.equal(timers.size,0);assert.equal(frames.size,0);});
  const emit=event=>channel?.onmessage?.({data:JSON.stringify(event)}),responses=()=>sent.filter(value=>value.type==='response.create');
  function begin(itemId='input-current'){
    emit({type:'input_audio_buffer.speech_started',item_id:itemId});emit({type:'input_audio_buffer.speech_stopped',item_id:itemId});emit({type:'input_audio_buffer.committed',item_id:itemId});return itemId;
  }
  function bind(responseId='response-current',request=responses().at(-1)){emit({type:'response.created',response:{id:responseId,metadata:request?.response.metadata}});return responseId;}
  function start(responseId='response-current'){emit({type:'output_audio_buffer.started',response_id:responseId});}
  function frame(){const entry=frames.entries().next().value;assert.ok(entry,'an analyser frame is scheduled');frames.delete(entry[0]);entry[1]();return levels.at(-1);}
  function attachRemote(amplitude=.1,bins=[[4,-20]]){const remote={amplitude,bins,getAudioTracks:()=>[track],getTracks:()=>[track]};peer.ontrack({streams:[remote]});return remote;}
  return {voice,runtime,document,channel:()=>channel,peer:()=>peer,audio:()=>audios.at(-1),contexts,analysers,frames,levels,states,playback,transcripts,listening,tools,frame,attachRemote,sent,requests,emit,begin,bind,start,responses};
}

test('shared measurement projects actual dB power into six finite amplitude-weighted bands',()=>{
  const f=direct({bins:[[2,-20],[4,-20],[8,-20],[16,-20],[32,-20],[64,-20]]}),signal=f.signal();valid(signal);near(signal.amplitude,.4);
  for(const band of signal.bands)near(band,.4/Math.sqrt(6));near(signal.brightness,(2+4+8+16+32+64)/(6*256));assert.equal(f.reads(),1);
  assert.equal(Object.hasOwn(signal,'emotion'),false);assert.equal(Object.hasOwn(signal,'pitch'),false);
});
test('spectrum follows actual low and high bins at equal amplitude, without temporal averaging',()=>{
  const f=direct({bins:[[2,-20]]}),low=f.signal();f.analyser.getFloatFrequencyData=values=>{values.fill(-Infinity);values[64]=-20;};const high=f.signal();valid(low);valid(high);near(low.amplitude,high.amplitude);
  assert.equal(low.bands[0],low.amplitude);assert.equal(high.bands[5],high.amplitude);near(low.brightness,2/256);near(high.brightness,64/256);assert.ok(high.brightness>low.brightness);
});
test('band shares use squared dB magnitude rather than transcript or arbitrary tone selection',()=>{
  const signal=direct({bins:[[2,-20],[64,-30]]}).signal();valid(signal);near(signal.bands[0],.4*Math.sqrt(10/11));near(signal.bands[5],.4*Math.sqrt(1/11));
  near(signal.brightness,(2*10+64)/(11*256));assert.equal(signal.bands.slice(1,5).every(value=>value===0),true);
});
test('waveform normalization is finite, capped, polarity-independent and independent of missing FFT',()=>{
  near(voiceApi.rms(new Float32Array([.1,-.1])),.4);assert.equal(voiceApi.rms(new Float32Array([1,-1])),1);assert.equal(voiceApi.rms([]),0);
  for(const bad of [NaN,Infinity,-Infinity])assert.equal(voiceApi.rms(new Float32Array([.1,bad])),0);
  const signal=direct({amplitude:1}).signal();valid(signal);assert.equal(signal.amplitude,1);
});
test('silent waveform cannot replay a prior energetic frequency buffer',()=>{
  const f=direct();valid(f.signal());f.waveform.fill(0);assert.deepEqual(f.signal(),NO_SIGNAL);assert.equal(f.reads(),1,'no current waveform means no spectral admission');
  f.waveform.fill(.1);f.analyser.getFloatFrequencyData=values=>values.fill(-Infinity);assert.deepEqual(f.signal(),NO_SIGNAL);
});
for(const issue of ['NaN','positive infinity','unwritten','throwing','underflow'])test('invalid '+issue+' spectrum clears every band without fabricated fallback',()=>{
  const f=direct({read(values){if(issue==='throwing')throw Error('private analyser detail');if(issue==='unwritten')return;values.fill(issue==='NaN'?NaN:issue==='positive infinity'?Infinity:-100000);}});
  assert.deepEqual(f.signal(),NO_SIGNAL);
});
for(const sampleRate of [NaN,Infinity,0,4000,192001,'48000'])test('unsupported sample rate '+String(sampleRate)+' reports unavailable spectrum',()=>{assert.deepEqual(direct({sampleRate}).signal(),NO_SIGNAL);});
test('malformed reusable buffers and destroyed analyser getters remain a safe optional failure',()=>{
  const f=direct();assert.deepEqual(voiceApi.measureOutputSignal(f.analyser,f.waveform,new Float32Array(3),48000),NO_SIGNAL);
  assert.deepEqual(voiceApi.measureOutputSignal(f.analyser,new Float32Array(2),f.frequencies,48000),NO_SIGNAL);
  Object.defineProperty(f.analyser,'fftSize',{get(){throw Error('destroyed analyser');}});assert.deepEqual(f.signal(),NO_SIGNAL);
});
test('shared network-route guard requires a gathered usable candidate for native QA and production',()=>{
  for(const value of [undefined,null,{},'v=0\r\nm=audio 9 UDP/TLS/RTP/SAVPF 111\r\na=end-of-candidates\r\n','a=candidate:bad 1 udp 1 192.0.2.10 0 typ host','a=candidate:bad 1 udp 1 192.0.2.10 65536 typ host','a=candidate:bad 0 udp 1 192.0.2.10 1000 typ host','a=candidate:bad 1 bad 1 192.0.2.10 1000 typ host'])assert.equal(voiceApi.hasVoiceNetworkRoute(value),false);
  for(const candidate of ['a=candidate:1 1 UDP 123 private-name.local 49152 typ host','a=candidate:2 2 udp 123 192.0.2.10 49152 typ srflx raddr 192.0.2.1 rport 1000','a=candidate:3 1 udp 123 2001:db8::1 49152 typ relay','a=candidate:4 1 tcp 123 192.0.2.10 9 typ host tcptype active'])assert.equal(voiceApi.hasVoiceNetworkRoute('v=0\r\n'+candidate+'\r\n'),true);
});

test('input and unattached response generation cannot animate a native output waveform',async t=>{
  const f=fixture(t);await f.voice.start();f.attachRemote();let sample=f.frame();near(sample.input,.8);assert.equal(sample.output,0);assert.deepEqual(sample.outputSignal,NO_SIGNAL);assert.equal(f.voice.currentOutput,null);
  f.begin();f.bind();f.emit({type:'response.output_audio_transcript.delta',response_id:'response-current',item_id:'output-part',delta:'These words have not played.'});sample=f.frame();assert.equal(sample.output,0);assert.deepEqual(sample.outputSignal,NO_SIGNAL);assert.equal(f.voice.currentOutput,null);
  f.start();sample=f.frame();valid(sample.outputSignal);near(sample.output,.4);assert.equal(sample.responseId,'response-current');assert.equal(sample.itemId,'output-part');assert.equal(sample.inputItemId,'input-current');assert.equal(sample.currentTurn,true);
  assert.equal(f.analysers[1].smoothingTimeConstant,0);assert.equal(f.analysers[1].fftSize,512);assert.equal(f.analysers[0].smoothingTimeConstant,.8);assert.equal(f.tools.length,0);assert.equal(f.requests.filter(value=>value.action==='start').length,1);
});
test('output binding is fresh read-only data and remains native rather than a device-audibility claim',async t=>{
  const f=fixture(t);await f.voice.start();f.begin();f.bind();f.start();const snapshot=f.voice.currentOutput;assert.equal(snapshot.responseId,'response-current');snapshot.responseId='forged';assert.equal(f.voice.currentOutput.responseId,'response-current');assert.notEqual(f.voice.currentOutput,snapshot);
  assert.equal(f.frame().output,0,'buffer lifecycle alone is not actual media samples');assert.equal(f.voice.state,'speaking');assert.equal(Object.hasOwn(snapshot,'audible'),false);assert.equal(Object.hasOwn(snapshot,'wordTimeMs'),false);
});
test('native signal changes every actual sampled frame, including a skipped media-clock interval',async t=>{
  const f=fixture(t);await f.voice.start();const stream=f.attachRemote(.1,[[2,-20]]);f.begin();f.bind();f.start();f.audio().currentTime=.5;const first=f.frame();valid(first.outputSignal);assert.equal(first.outputTimeMs,500);assert.equal(first.outputSignal.bands[0],first.outputSignal.amplitude);
  stream.bins=[[64,-20]];stream.amplitude=.08;f.audio().currentTime=1.8;const next=f.frame();valid(next.outputSignal);assert.equal(next.outputTimeMs,1800);near(next.outputSignal.amplitude,.32);assert.equal(next.outputSignal.bands[5],next.outputSignal.amplitude);assert.equal(next.outputSignal.bands[0],0);
  stream.amplitude=0;const silence=f.frame();assert.equal(silence.output,0);assert.deepEqual(silence.outputSignal,NO_SIGNAL);assert.equal(f.voice.state,'speaking','actual native buffer still controls talking lifecycle');
});
test('invalid local media time omits a clock but never blocks current real spectrum or identity',async t=>{
  const f=fixture(t);await f.voice.start();f.attachRemote();f.begin();f.bind();f.start();f.audio().currentTime=.6;assert.equal(f.frame().outputTimeMs,600);
  for(const time of [NaN,Infinity,-1,601,.5]){f.audio().currentTime=time;const sample=f.frame();valid(sample.outputSignal);assert.equal(sample.responseId,'response-current');assert.equal(Object.hasOwn(sample,'outputTimeMs'),false);}
  Object.defineProperty(f.audio(),'currentTime',{get(){throw Error('private media getter');}});valid(f.frame().outputSignal);assert.equal(f.voice.state,'speaking');
});
for(const boundary of ['paused','ended','hidden','disconnected','blocked','suspended','interrupted','closed context'])test(boundary+' clears real signal until the same current media becomes usable',async t=>{
  const f=fixture(t);await f.voice.start();f.attachRemote();f.begin();f.bind();f.start();valid(f.frame().outputSignal);
  if(boundary==='paused')f.audio().paused=true;
  else if(boundary==='ended')f.audio().ended=true;
  else if(boundary==='hidden')f.document.hidden=true;
  else if(boundary==='disconnected'){f.peer().connectionState='disconnected';f.peer().onconnectionstatechange();assert.deepEqual(f.levels.at(-1),NO_LEVELS);}
  else if(boundary==='blocked'){f.audio().play=()=>Promise.reject(Error('synthetic autoplay gate'));assert.equal(await f.voice.resumeAudio(),false);assert.deepEqual(f.levels.at(-1),NO_LEVELS);}
  else f.contexts[0].state=boundary==='closed context'?'closed':boundary;
  const sample=f.frame();assert.equal(sample.output,0);assert.deepEqual(sample.outputSignal,NO_SIGNAL);assert.equal(f.voice.state,'speaking');assert.equal(f.requests.filter(value=>value.action==='stop').length,0);
  if(boundary==='paused')f.audio().paused=false;
  else if(boundary==='ended')f.audio().ended=false;
  else if(boundary==='hidden')f.document.hidden=false;
  else if(boundary==='disconnected'){f.peer().connectionState='connected';f.peer().onconnectionstatechange();}
  else if(boundary==='blocked'){f.audio().play=function(){this.paused=false;return Promise.resolve();};assert.equal(await f.voice.resumeAudio(),true);}
  else f.contexts[0].state='running';
  valid(f.frame().outputSignal);assert.equal(f.requests.filter(value=>value.action==='start').length,1);
});
test('spectrum support is optional; missing or failing FFT cannot stop native voice or invent bands',async t=>{
  const f=fixture(t,{spectrum:false});await f.voice.start();f.attachRemote();f.begin();f.bind();f.start();const sample=f.frame();near(sample.output,.4);assert.deepEqual(sample.outputSignal,NO_SIGNAL);assert.equal(f.voice.state,'speaking');assert.equal(f.voice.outputMeterState,'ready');
});
for(const issue of ['frequency throw','frequency unwritten','waveform throw','waveform unwritten'])test(issue+' clears current signal while native media keeps playing',async t=>{
  const f=fixture(t);await f.voice.start();f.attachRemote();f.begin();f.bind();f.start();valid(f.frame().outputSignal);const analyser=f.analysers[1],key=issue.startsWith('frequency')?'getFloatFrequencyData':'getFloatTimeDomainData';analyser[key]=()=>{if(issue.endsWith('throw'))throw Error('private analyser detail');};
  const sample=f.frame();assert.deepEqual(sample.outputSignal,NO_SIGNAL);if(key==='getFloatTimeDomainData')assert.equal(sample.output,0);assert.equal(f.voice.state,'speaking');assert.equal(f.requests.filter(value=>value.action==='start').length,1);assert.equal(f.requests.filter(value=>value.action==='stop').length,0);
});
for(const eventType of ['output_audio_buffer.stopped','output_audio_buffer.cleared'])test(eventType+' drains only the actual buffer and rejects late same-response starts',async t=>{
  const f=fixture(t);await f.voice.start();f.attachRemote();f.begin();f.bind();f.start();valid(f.frame().outputSignal);f.emit({type:'response.done',response:{id:'response-current',status:'completed'}});assert.equal(f.voice.state,'speaking');valid(f.frame().outputSignal);
  f.emit({type:eventType,response_id:'response-current'});assert.deepEqual(f.levels.at(-1),NO_LEVELS);assert.equal(f.voice.currentOutput,null);assert.equal(f.voice.state,'listening');const count=f.playback.length;f.start();assert.equal(f.playback.length,count);assert.equal(f.voice.currentOutput,null);assert.deepEqual(f.frame().outputSignal,NO_SIGNAL);
});
for(const boundary of ['interrupt','new input','stop','dispose'])test(boundary+' invalidates old response samples, callbacks and native-output identity',async t=>{
  const f=fixture(t);await f.voice.start();f.attachRemote();f.begin();f.bind();f.start();valid(f.frame().outputSignal);const handler=f.channel().onmessage,callback=f.frames.values().next().value;
  if(boundary==='interrupt')f.voice.interrupt();else if(boundary==='new input')f.emit({type:'input_audio_buffer.speech_started',item_id:'next-input'});else if(boundary==='stop')await f.voice.stop();else await f.voice.dispose();
  assert.deepEqual(f.levels.at(-1),NO_LEVELS);assert.equal(f.voice.currentOutput,null);const count=f.playback.length;handler({data:JSON.stringify({type:'output_audio_buffer.started',response_id:'response-current'})});assert.equal(f.playback.length,count);
  if(boundary==='stop'||boundary==='dispose'){const count=f.levels.length;callback();assert.equal(f.levels.length,count);assert.equal(f.frames.size,0);}else assert.deepEqual(f.frame().outputSignal,NO_SIGNAL);
});
test('catalogue tools and a newer same-turn generated response preserve the still-playing old native buffer',async t=>{
  let complete;const result=new Promise(resolve=>complete=resolve),f=fixture(t,{onTool:()=>result});await f.voice.start();f.attachRemote();f.begin();f.bind('audible-response');f.start('audible-response');valid(f.frame().outputSignal);
  f.emit({type:'response.function_call_arguments.done',response_id:'audible-response',call_id:'catalogue-call',name:'inspect_jewellery',arguments:JSON.stringify({handle:'bunny-necklace'})});
  assert.equal(f.tools.length,1);assert.equal(f.voice.state,'speaking','catalogue pending state cannot stop an actually playing mouth');assert.equal(f.voice.currentOutput.responseId,'audible-response');valid(f.frame().outputSignal);
  complete({verified:true,product:{handle:'bunny-necklace'}});await tick();assert.equal(f.voice.state,'speaking');assert.equal(f.responses().length,2);f.bind('next-generated');f.emit({type:'response.output_audio_transcript.delta',response_id:'next-generated',item_id:'next-output',delta:'A verified jewellery option.'});
  assert.equal(f.voice.currentOutput.responseId,'audible-response');const stillPlaying=f.frame();valid(stillPlaying.outputSignal);assert.equal(stillPlaying.responseId,'audible-response');
  f.emit({type:'output_audio_buffer.stopped',response_id:'next-generated'});assert.equal(f.voice.currentOutput.responseId,'audible-response','an unrelated next response stop cannot drain the old audible buffer');
  f.emit({type:'response.done',response:{id:'audible-response',status:'completed'}});assert.equal(f.voice.state,'speaking');valid(f.frame().outputSignal);
  f.emit({type:'output_audio_buffer.stopped',response_id:'audible-response'});assert.equal(f.voice.currentOutput,null);assert.deepEqual(f.frame().outputSignal,NO_SIGNAL);assert.equal(f.requests.filter(value=>value.action==='start').length,1);
});
test('only a different actual buffer start retires prior same-turn output and rejects a late duplicate start',async t=>{
  const f=fixture(t);await f.voice.start();f.attachRemote();f.begin();f.bind('audible-A');f.start('audible-A');valid(f.frame().outputSignal);
  f.emit({type:'response.function_call_arguments.done',response_id:'audible-A',call_id:'next-response-tool',name:'inspect_jewellery',arguments:JSON.stringify({handle:'bunny-necklace'})});await tick();f.bind('generated-B');f.emit({type:'response.output_audio_transcript.delta',response_id:'generated-B',item_id:'output-B',delta:'The next response was generated.'});
  assert.equal(f.voice.currentOutput.responseId,'audible-A','generation and transcript B preserve still-playing A');valid(f.frame().outputSignal);f.start('generated-B');assert.equal(f.voice.currentOutput.responseId,'generated-B');const count=f.playback.length;
  f.start('audible-A');assert.equal(f.voice.currentOutput.responseId,'generated-B');assert.equal(f.playback.length,count);f.emit({type:'output_audio_buffer.stopped',response_id:'audible-A'});assert.equal(f.voice.currentOutput.responseId,'generated-B');const sample=f.frame();valid(sample.outputSignal);assert.equal(sample.responseId,'generated-B');assert.equal(f.voice.state,'speaking');
});
test('the next actual native buffer replaces metadata without keeping older local clock or stop authority',async t=>{
  const f=fixture(t);await f.voice.start();f.attachRemote();f.begin('old-input');f.bind('old-response');f.start('old-response');f.audio().currentTime=1.2;valid(f.frame().outputSignal);
  f.begin('new-input');f.bind('new-response');f.start('new-response');f.audio().currentTime=.1;const sample=f.frame();valid(sample.outputSignal);assert.equal(sample.inputItemId,'new-input');assert.equal(sample.responseId,'new-response');assert.equal(sample.outputTimeMs,100);
  f.emit({type:'output_audio_buffer.stopped',response_id:'old-response'});f.start('old-response');assert.equal(f.voice.currentOutput.responseId,'new-response');valid(f.frame().outputSignal);
});
test('a prior-session RAF callback cannot publish or create a second meter loop after explicit restart',async t=>{
  const f=fixture(t);await f.voice.start();f.attachRemote();f.begin('old-input');f.bind('old-response');f.start('old-response');valid(f.frame().outputSignal);const callback=f.frames.values().next().value,handler=f.channel().onmessage;await f.voice.stop();
  await f.voice.start();f.attachRemote(.08,[[64,-20]]);f.begin('new-input');f.bind('new-response');f.start('new-response');const before={levels:f.levels.length,frames:f.frames.size,playback:f.playback.length};
  callback();handler({data:JSON.stringify({type:'output_audio_buffer.started',response_id:'old-response'})});assert.deepEqual({levels:f.levels.length,frames:f.frames.size,playback:f.playback.length},before);assert.equal(f.frames.size,1);
  const current=f.frame();valid(current.outputSignal);assert.equal(current.responseId,'new-response');near(current.outputSignal.amplitude,.32);assert.equal(f.frames.size,1);assert.equal(f.requests.filter(value=>value.action==='start').length,2,'two explicit sessions, without an automatic retry');
});
test('input ASR delta remains presentation-only while output admission follows native audio',async t=>{
  const f=fixture(t);await f.voice.start();f.attachRemote();f.begin();f.bind();f.emit({type:'conversation.item.input_audio_transcription.delta',item_id:'input-current',delta:'show the bunny necklace'});
  assert.equal(f.listening.length,1);assert.equal(f.tools.length,0);assert.equal(f.transcripts.length,0);assert.deepEqual(f.frame().outputSignal,NO_SIGNAL);assert.equal(f.voice.currentOutput,null);assert.equal(f.responses().length,1);
});
