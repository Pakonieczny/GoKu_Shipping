'use strict';

// The native QA page executes in JSDOM's VM with the production speech meter,
// expression planner and SVG fallback rig. Device, transport and time events
// are synthetic. These tests establish lifecycle behavior, not provider audio,
// physical audibility, GPU rendering or perceived voice/avatar fit.
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs');
const {JSDOM,VirtualConsole}=require('jsdom');
const Voice=require('../../brites-concierge-voice.js');
const Expression=require('../../brites-concierge-expression.js');
const Avatar=require('../../brites-concierge-avatar.js');
const source=fs.readFileSync(require.resolve('../../concierge-voice-qa.js'),'utf8');
const html=fs.readFileSync(require.resolve('../../concierge-voice-qa.html'),'utf8');
const SDP='v=0\r\nm=audio 9 UDP/TLS/RTP/SAVPF 111\r\na=candidate:1 1 UDP 2122260223 192.0.2.20 50000 typ host\r\n';
const ZERO={amplitude:0,bands:[0,0,0,0,0,0],brightness:0,valid:false};
const settle=async()=>{await new Promise(resolve=>setImmediate(resolve));await new Promise(resolve=>setImmediate(resolve));};
function deferred(){let resolve,reject;const promise=new Promise((yes,no)=>{resolve=yes;reject=no;});return {promise,resolve,reject};}
const copy=value=>JSON.parse(JSON.stringify(value));

function fixture(t,options={}){
  const errors=[],virtualConsole=new VirtualConsole();virtualConsole.on('jsdomError',error=>errors.push(error));
  const dom=new JSDOM(html,{url:'https://native-qa.example/concierge-voice-qa.html',runScripts:'outside-only',pretendToBeVisual:true,virtualConsole});
  const w=dom.window,d=w.document,audio=d.querySelector('#audio');
  let clock=0,hidden=false,paused=true,ended=false,audioTime=0,audioObject=null,serial=0,microphones=0,loadIndex=0;
  const playResults=[...options.playResults||[]];
  const timers=new Map(),frames=new Map(),requests=[],peers=[],contexts=[],guides=[],planners=[],signals=[],levels=[],spectrumReads=[];
  Object.defineProperty(d,'hidden',{get:()=>hidden});
  Object.defineProperty(w.performance,'now',{value:()=>clock});
  Object.defineProperty(audio,'paused',{get:()=>paused,set:value=>{paused=value;}});
  Object.defineProperty(audio,'ended',{get:()=>ended,set:value=>{ended=value;}});
  Object.defineProperty(audio,'currentTime',{get:()=>audioTime,set:value=>{audioTime=value;}});
  Object.defineProperty(audio,'srcObject',{get:()=>audioObject,set:value=>{audioObject=value;if(value&&options.resetOnAttach)audioTime=0;}});
  audio.play=()=>{if(options.playBlocked)return Promise.reject(Error('Synthetic blocked playback'));paused=false;return playResults.shift()?.promise||Promise.resolve();};
  audio.pause=()=>{paused=true;};
  w.matchMedia=()=>({matches:false,addEventListener(){},removeEventListener(){}});
  w.setTimeout=(fn,ms=0)=>{const id=++serial;timers.set(id,{fn,ms:Number(ms),at:clock+Number(ms)});return id;};
  w.clearTimeout=id=>timers.delete(id);
  w.requestAnimationFrame=fn=>{const id=++serial;frames.set(id,fn);return id;};
  w.cancelAnimationFrame=id=>frames.delete(id);
  w.AbortController=AbortController;
  w.AbortSignal={timeout(){return new AbortController().signal;}};
  Object.defineProperty(w.navigator,'mediaDevices',{value:{getUserMedia(){microphones++;return Promise.reject(Error('Microphones are forbidden in this fixture'));}}});

  class Peer{
    constructor(){peers.push(this);this.connectionState='new';this.iceConnectionState='new';this.iceGatheringState=options.gathering?'gathering':'complete';this.listeners=new Map();this.transceivers=[];this.closed=0;this.remoteDescriptions=[];}
    addEventListener(name,fn){if(!this.listeners.has(name))this.listeners.set(name,new Set());this.listeners.get(name).add(fn);}
    removeEventListener(name,fn){this.listeners.get(name)?.delete(fn);}
    emit(name,event={}){for(const fn of [...this.listeners.get(name)||[]])fn(event);}
    addTransceiver(kind,value){this.transceivers.push({kind,...value});}
    createDataChannel(name){const channel={name,readyState:'connecting',sent:[],closed:0,send(value){this.sent.push(JSON.parse(value));},close(){this.closed++;this.readyState='closed';}};this.channel=channel;return channel;}
    async createOffer(){if(options.offer)await options.offer.promise;return {type:'offer',sdp:options.emptyRoute?'v=0\r\nm=audio 9 UDP/TLS/RTP/SAVPF 111\r\n':SDP};}
    async setLocalDescription(value){this.localDescription=value;}
    async setRemoteDescription(value){this.remoteDescriptions.push(value);if(options.remoteAnswer)await options.remoteAnswer.promise;this.connectionState='connected';this.iceConnectionState='connected';this.channel.readyState='open';this.channel.onopen?.();}
    close(){this.closed++;this.connectionState='closed';this.channel?.close();}
  }
  class AudioContext{
    constructor(){contexts.push(this);this.state='suspended';this.sampleRate=48000;this.closed=0;}
    resume(){this.state='running';return Promise.resolve();}
    close(){this.closed++;this.state='closed';return Promise.resolve();}
    createMediaStreamSource(remote){return {connect(analyser){analyser.remote=remote;},disconnect(){}};}
    createAnalyser(){return {
      fftSize:2048,smoothingTimeConstant:0,
      get frequencyBinCount(){return this.fftSize/2;},
      getFloatTimeDomainData(samples){const remote=this.remote;for(let i=0;i<samples.length;i++)samples[i]=remote.invalidWave?NaN:remote.peak*Math.sin(2*Math.PI*remote.bin*i/samples.length);},
      getFloatFrequencyData(frequencies){spectrumReads.push(this.remote);if(this.remote.spectrumThrows)throw Error('Synthetic FFT failure');frequencies.fill(-Infinity);if(this.remote.invalidSpectrum)frequencies[this.remote.bin]=NaN;else if(!this.remote.emptySpectrum)frequencies[this.remote.bin]=-24;},disconnect(){}};}
  }
  w.RTCPeerConnection=Peer;w.AudioContext=AudioContext;
  w.MediaStream=class{constructor(tracks){this.tracks=tracks;this.peak=.075;this.bin=5;}};
  w.BritesConciergeVoice=Voice;
  w.BritesConciergeExpression={...Expression,create(value){const planner=Expression.create(value);planners.push(planner);const update=planner.level.bind(planner);planner.level=value=>{levels.push(copy(value));return update(value);};return planner;}};
  w.BritesConciergeAvatar={create(value){const guide=Avatar.create({...value,loadScene:async()=>{throw Error('Synthetic unavailable WebGL');}});guides.push(guide);const write=guide.setSpeechSignal.bind(guide);guide.setSpeechSignal=value=>{signals.push(copy(value));return write(value);};return guide;}};
  const append=d.head.append.bind(d.head);
  d.head.append=(...nodes)=>{append(...nodes);for(const node of nodes)if(node.tagName==='SCRIPT'){const load=()=>node.dispatchEvent(new w.Event('load')),fail=()=>node.dispatchEvent(new w.Event('error')),result=options.loadResults?.[loadIndex++];if(result==='error')queueMicrotask(fail);else if(result?.promise)result.promise.then(load,fail);else if(options.loader)options.loader.promise.then(load,fail);else queueMicrotask(load);}};
  w.fetch=async(url,init)=>{
    const body=JSON.parse(init.body);requests.push({url:String(url),body,credentials:init.credentials,redirect:init.redirect});
    let value;if(body.action==='capabilities')value=options.capabilities?await options.capabilities.promise:{enabled:true,demoToken:'synthetic-demo-token'};
    else if(body.action==='start')value=options.answer?await options.answer.promise:{enabled:true,sdp:SDP,stopToken:'synthetic-stop-token'};
    else if(body.action==='stop')value=options.stopAnswer?await options.stopAnswer.promise:{stopped:true};
    else throw Error('Unexpected synthetic request');
    return {ok:true,json:async()=>value};
  };
  w.eval(source);
  function facts(){return JSON.parse(d.querySelector('#signal-evidence').textContent);}
  function frame(ms=50){clock+=ms;if(!paused&&Number.isFinite(audioTime))audioTime+=ms/1000;const due=[...timers].filter(([,value])=>value.at<=clock).sort((a,b)=>a[1].at-b[1].at);for(const [id,value] of due)if(timers.delete(id))value.fn();const scheduled=[...frames];for(const [id,fn] of scheduled)if(frames.delete(id))fn(clock);return facts();}
  function advance(ms){for(let remaining=ms;remaining>0;remaining-=50)frame(Math.min(50,remaining));return facts();}
  const h={w,d,audio,frames,timers,requests,peers,contexts,guides,planners,signals,levels,spectrumReads,errors,frame,advance,facts,
    get peer(){return peers.at(-1);},get guide(){return guides.at(-1);},get planner(){return planners.at(-1);},get microphones(){return microphones;},
    async start(){d.querySelector('#start').click();await settle();},async stop(){d.querySelector('#stop').click();await settle();},
    emit(event,channel=h.peer?.channel){channel?.onmessage?.({data:JSON.stringify(event)});},
    bind(id='native-response'){h.emit({type:'response.created',response:{id}});return id;},
    attach(value={}){const remote={peak:.075,bin:5,...value};h.peer.ontrack({streams:[remote],track:{kind:'audio',readyState:'live'}});return remote;},
    buffer(id='native-response'){h.emit({type:'output_audio_buffer.started',response_id:id});},
    hide(value){hidden=value;d.dispatchEvent(new w.Event('visibilitychange'));},
    actions(action){return requests.filter(value=>value.body.action===action);}
  };
  t.after(async()=>{if(!d.querySelector('#stop').disabled)await h.stop();for(const planner of planners)planner.destroy();for(const guide of guides)guide.destroy();timers.clear();frames.clear();w.close();assert.equal(microphones,0,'the native receiver never requests a microphone');assert.ok(requests.every(value=>value.url==='/api/concierge-voice'&&value.credentials==='same-origin'&&value.redirect==='error'));assert.deepEqual(errors,[]);});
  return h;
}
async function speaking(t,options={}){const h=fixture(t,options);await h.start();h.bind();h.attach();h.buffer();h.frame();h.frame();return h;}
function assertSilent(h,message){const evidence=h.facts();assert.deepEqual(evidence.speechSignal,ZERO,message);assert.equal(Number(h.d.querySelector('#level').value),0);if(h.guide){assert.equal(h.guide.snapshot().speechSignal.amplitude,0);assert.equal(h.guide.snapshot().speechVisual.rippleActive,false);assert.equal(h.guide.snapshot().facePose.mouthOpen,0);}}

test('the optional page is inert before a deliberate start and requests only receive audio',async t=>{
  const h=fixture(t);assert.equal(h.requests.length,0);assert.equal(h.guides.length,0);assert.equal(h.contexts.length,0);
  await h.start();assert.deepEqual(h.peer.transceivers,[{kind:'audio',direction:'recvonly'}]);assert.equal(h.actions('start').length,1);assert.equal(h.peer.channel.sent.length,2);
  const request=h.peer.channel.sent.find(value=>value.type==='response.create');assert.equal(request.response.tool_choice,'none');assert.ok(request.response.max_output_tokens>0&&request.response.max_output_tokens<=300);
  const turn=h.peer.channel.sent.find(value=>value.type==='conversation.item.create').item;assert.equal(turn.role,'user');assert.ok(turn.content.every(value=>value.type==='input_text'));assert.equal(h.microphones,0);
});

test('a remote waveform and generated transcript cannot animate speech before its native buffer starts',async t=>{
  const h=fixture(t);await h.start();h.bind();h.attach();h.emit({type:'response.output_audio_transcript.done',response_id:'native-response',item_id:'native-output',content_index:0,transcript:'Take your time. Is this a gift?'});h.advance(300);
  assertSilent(h,'positive raw remote audio is not a playing native response');assert.equal(h.spectrumReads.length,0);assert.equal(h.planner.snapshot().playing,false);assert.equal(h.guide.snapshot().state,'thinking');assert.equal(h.actions('start').length,1);
  assert.match(h.d.querySelector('#evidence').textContent,/Peak normalized audio level: 0\.2121/);assert.equal(h.facts().positiveSignalSamples,0);
});

test('current connected and unpaused native output reaches the production meter and closed-mouth rig',async t=>{
  const h=await speaking(t);const before=h.facts();assert.equal(before.speechSignal.valid,true);assert.ok(Math.abs(before.speechSignal.amplitude-.075*Math.SQRT1_2*4)<1e-6);assert.equal(before.speechSignal.bands.filter(value=>value>0).length,1);assert.ok(before.speechSignal.bands[1]>0);assert.ok(h.spectrumReads.length>0);
  const face=h.guide.snapshot();assert.equal(face.state,'speaking');assert.equal(face.mode,'fallback');assert.equal(face.speechVisual.curveClosed,true);assert.equal(face.speechVisual.rippleActive,true);assert.ok(face.speechVisual.amplitude>0);assert.equal(face.facePose.mouthOpen,0);assert.equal(h.planner.snapshot().playing,true);assert.equal(h.microphones,0);
});

test('the authored focus tray does not suppress an ongoing native speech spectrum',async t=>{
  const h=await speaking(t);const before=h.facts().positiveSignalSamples;h.advance(550);
  const result=h.facts();assert.equal(h.d.querySelector('#fixture-tray').hidden,false);assert.equal(result.focusDuringOutput,true);assert.ok(result.positiveAfterFocus>0);assert.ok(result.positiveSignalSamples>before);assert.equal(result.speechSignal.valid,true);assert.ok(result.speechSignal.amplitude>0);assert.equal(h.guide.snapshot().state,'speaking');assert.ok(h.guide.snapshot().productFocus);assert.equal(h.guide.snapshot().speechVisual.rippleActive,true);assert.equal(h.guide.snapshot().facePose.mouthOpen,0);assert.equal(h.actions('start').length,1);assert.equal(h.actions('stop').length,0);
  h.spectrumReads.at(-1).bin=40;h.frame();assert.ok(h.facts().speechSignal.bands[4]>0,'the signal continues to reflect fresh FFT bins after focus');assert.equal(h.facts().speechSignal.bands[1],0);
});

test('an unchecked focus option keeps the fixture tray closed without stopping speech',async t=>{
  const h=fixture(t);h.d.querySelector('#focus-during-speech').checked=false;await h.start();h.bind();h.attach();h.buffer();h.advance(600);assert.equal(h.d.querySelector('#fixture-tray').hidden,true);assert.equal(h.facts().focusDuringOutput,false);assert.ok(h.facts().speechSignal.amplitude>0);assert.equal(h.guide.snapshot().state,'speaking');
});

for(const invalid of ['invalidSpectrum','emptySpectrum','spectrumThrows','invalidWave'])test(invalid+' cannot substitute an invented spectrum for native audio',async t=>{
  const h=fixture(t);await h.start();h.bind();h.attach({[invalid]:true});h.buffer();h.frame();assertSilent(h,invalid);assert.equal(h.facts().positiveSignalSamples,0);assert.equal(h.facts().positiveRigSamples,0);assert.equal(h.actions('start').length,1);
});

for(const boundary of ['paused','ended','suspended','disconnected'])test(boundary+' playback immediately excludes raw remote energy from the avatar',async t=>{
  const h=await speaking(t);assert.ok(h.facts().speechSignal.amplitude>0);
  if(boundary==='paused')h.audio.paused=true;else if(boundary==='ended')h.audio.ended=true;else if(boundary==='suspended')h.contexts.at(-1).state='suspended';else{h.peer.connectionState='disconnected';h.peer.emit('connectionstatechange');assertSilent(h,'transport callback clears without waiting for RAF');}
  h.frame();assertSilent(h,boundary);assert.equal(h.actions('start').length,1);assert.equal(h.actions('stop').length,0);
});

test('blocked autoplay never admits raw audio or opens a second provider session',async t=>{
  const h=fixture(t,{playBlocked:true});await h.start();h.bind();h.attach();await settle();h.buffer();h.advance(250);assertSilent(h,'browser playback is blocked');assert.equal(h.facts().positiveSignalSamples,0);assert.equal(h.actions('start').length,1);assert.equal(h.actions('stop').length,1);assert.equal(h.guide.snapshot().state,'idle');assert.match(h.d.querySelector('#status').textContent,/playback was blocked/,'late buffer callbacks cannot overwrite the blocked-playback explanation');
});

for(const type of ['output_audio_buffer.stopped','output_audio_buffer.cleared'])test(type+' clears signal synchronously and late meter callbacks cannot restore it',async t=>{
  const h=await speaking(t);const samples=h.facts().positiveSignalSamples;h.emit({type,response_id:'native-response'});assertSilent(h,type);h.advance(500);assertSilent(h,'raw remote audio persists after provider stop');assert.equal(h.facts().positiveSignalSamples,samples);assert.equal(h.d.querySelector('#fixture-tray').hidden,true);h.advance(350);await settle();assert.equal(h.actions('stop').length,1);assert.equal(h.peer.closed,1);assert.equal(h.actions('start').length,1);
});

test('foreign response buffer events cannot claim or clear the current native output',async t=>{
  const h=fixture(t);await h.start();h.bind();h.attach();h.buffer('old-response');h.frame();assertSilent(h,'old buffer did not start current output');h.buffer();h.frame();assert.ok(h.facts().speechSignal.amplitude>0);h.emit({type:'output_audio_buffer.stopped',response_id:'old-response'});h.frame();assert.ok(h.facts().speechSignal.amplitude>0);assert.equal(h.guide.snapshot().state,'speaking');assert.equal(h.actions('stop').length,0);
});

test('explicit end hangs up and old channel/frame callbacks stay silent without restart',async t=>{
  const h=await speaking(t),handler=h.peer.channel.onmessage,oldFrames=[...h.frames.values()],samples=h.facts().positiveSignalSamples;await h.stop();assertSilent(h,'explicit end');assert.equal(h.guide.snapshot().state,'idle');assert.equal(h.peer.closed,1);assert.equal(h.contexts.at(-1).closed,1);assert.equal(h.audio.srcObject,null);assert.equal(h.audio.paused,true);assert.equal(h.actions('stop').length,1);
  handler({data:JSON.stringify({type:'output_audio_buffer.started',response_id:'native-response'})});for(const fn of oldFrames)fn(900);h.advance(1000);assertSilent(h,'callbacks from the ended epoch');assert.equal(h.facts().positiveSignalSamples,samples);assert.equal(h.actions('start').length,1);assert.equal(h.actions('stop').length,1);assert.equal(h.d.querySelector('#start').disabled,false);
});

for(const boundary of ['hidden','pagehide'])test(boundary+' ends native playback and returning to the page never restarts it',async t=>{
  const h=await speaking(t),oldTrack=h.peer.ontrack;if(boundary==='hidden')h.hide(true);else h.w.dispatchEvent(new h.w.Event('pagehide'));assertSilent(h,boundary);await settle();assert.equal(h.peer.closed,1);assert.equal(h.actions('stop').length,1);oldTrack({streams:[{peak:.9,bin:40}],track:{kind:'audio'}});h.hide(false);h.advance(1000);assertSilent(h,'returning visibility is not an instruction to speak');assert.equal(h.audio.srcObject,null);assert.equal(h.actions('start').length,1);assert.equal(h.facts().positiveAfterFocus,0);
});

test('canceling pending capabilities releases setup and never creates a provider call',async t=>{
  const capabilities=deferred(),h=fixture(t,{capabilities});await h.start();assert.equal(h.actions('capabilities').length,1);await h.stop();capabilities.resolve({enabled:true,demoToken:'late-demo-token'});await settle();h.advance(1000);assertSilent(h,'late capabilities');assert.equal(h.peers.length,0);assert.equal(h.actions('start').length,0);assert.equal(h.actions('stop').length,0);assert.equal(h.contexts.at(-1).closed,1);assert.equal(h.d.querySelector('#start').disabled,false);
});

test('canceling a pending browser offer cannot create a call after setup resumes',async t=>{
  const offer=deferred(),h=fixture(t,{offer});await h.start();assert.equal(h.peers.length,1);await h.stop();offer.resolve();await settle();h.advance(1000);assertSilent(h,'late browser offer');assert.equal(h.actions('start').length,0);assert.equal(h.actions('stop').length,0);assert.equal(h.peer.closed,1);assert.equal(h.contexts.at(-1).closed,1);assert.equal(h.d.querySelector('#start').disabled,false);
});

test('canceling candidate gathering removes its listener and cannot allocate a call later',async t=>{
  const h=fixture(t,{gathering:true});await h.start();assert.equal(h.actions('start').length,0);assert.ok(h.peer.listeners.get('icegatheringstatechange').size>1);await h.stop();h.peer.iceGatheringState='complete';h.peer.emit('icegatheringstatechange');await settle();h.advance(1000);assertSilent(h,'canceled candidate gathering');assert.equal(h.actions('start').length,0);assert.equal(h.actions('stop').length,0);assert.equal(h.peer.listeners.get('icegatheringstatechange').size,1,'the transient gather listener was removed');
});

test('a provider answer received after cancellation is hung up without applying it or retrying',async t=>{
  const answer=deferred(),h=fixture(t,{answer});await h.start();assert.equal(h.actions('start').length,1);await h.stop();assert.equal(h.actions('stop').length,0);answer.resolve({enabled:true,sdp:SDP,stopToken:'late-synthetic-stop-token'});await settle();assertSilent(h,'late provider answer');assert.equal(h.actions('stop').length,1);assert.equal(h.actions('stop')[0].body.stopToken,'late-synthetic-stop-token');assert.equal(h.peer.remoteDescriptions.length,0);assert.equal(h.peer.closed,1);assert.equal(h.actions('start').length,1);h.advance(21000);await settle();assert.equal(h.actions('start').length,1);assert.equal(h.actions('stop').length,1);
});

test('a canceled pending remote description cannot arm a deadline for a later page state',async t=>{
  const remoteAnswer=deferred(),h=fixture(t,{remoteAnswer});await h.start();assert.equal(h.peer.remoteDescriptions.length,1);await h.stop();remoteAnswer.resolve();await settle();assertSilent(h,'late remote description');assert.equal(h.actions('stop').length,1);assert.equal(h.peer.channel.sent.length,0);assert.equal([...h.timers.values()].filter(value=>value.ms===20000).length,0,'an ended setup must not register a fresh native-session deadline');assert.equal(h.actions('start').length,1);
});

for(const time of [-1,601,Number.MAX_VALUE,NaN,Infinity])test('invalid native media time '+String(time)+' cannot authorize a positive output signal',async t=>{
  const h=fixture(t);await h.start();h.bind();h.attach();h.audio.currentTime=time;h.buffer();h.frame(0);assertSilent(h,'invalid native clock');assert.equal(h.facts().positiveSignalSamples,0);assert.equal(h.actions('start').length,1);
});

test('a replacement remote stream owns a new media baseline and stale meter frames cannot erase it',async t=>{
  const h=await speaking(t);h.audio.currentTime=5;h.frame(0);assert.ok(h.facts().speechSignal.amplitude>0);const staleFrames=[...h.frames.values()];h.audio.currentTime=.02;h.attach({peak:.1,bin:40});h.frame(30);
  assert.equal(h.facts().speechSignal.valid,true,'replacement media may restart its own currentTime');assert.ok(h.facts().speechSignal.bands[4]>0);assert.equal(h.guide.snapshot().state,'speaking');const writes=h.signals.length;for(const fn of staleFrames)fn(1000);assert.equal(h.signals.length,writes,'obsolete stream callbacks must not clear or reschedule current analysis');assert.ok(h.facts().speechSignal.amplitude>0);assert.equal(h.actions('start').length,1);assert.equal(h.actions('stop').length,0);
});

test('a canceled avatar asset load cannot create a late speaking rig or provider call',async t=>{
  const loader=deferred(),h=fixture(t,{loader});await h.start();assert.equal(h.guides.length,0);await h.stop();loader.resolve();await settle();assert.equal(h.guides.length,0,'the retired load must not construct an avatar after End');assert.equal(h.actions('capabilities').length,0);assert.equal(h.actions('start').length,0);assert.equal(h.contexts.at(-1).closed,1);assertSilent(h,'retired avatar load');
});

test('intentional restart during a pending avatar asset load installs one current rig and one call',async t=>{
  const loader=deferred(),h=fixture(t,{loader});await h.start();await h.stop();await h.start();assert.equal(h.contexts.length,2);loader.resolve();await settle();assert.equal(h.guides.length,1);assert.equal(h.d.querySelector('#avatar-mount').children.length,1);assert.equal(h.actions('capabilities').length,1);assert.equal(h.actions('start').length,1);assert.equal(h.contexts[0].closed,1);assert.equal(h.contexts[1].state,'running');h.bind();h.attach();h.buffer();h.advance(150);assert.ok(h.facts().speechSignal.amplitude>0);assert.equal(h.guide.snapshot().speechVisual.rippleActive,true);assert.equal(h.guide.snapshot().facePose.mouthOpen,0);
});

for(const stopped of [true,false])test('overlapping End/hide/pagehide preserves one pending hangup and its '+stopped+' evidence',async t=>{
  const stopAnswer=deferred(),h=await speaking(t,{stopAnswer});await h.stop();assertSilent(h,'local silence does not wait for server hangup');assert.equal(h.actions('stop').length,1);assert.equal(h.d.querySelector('#start').disabled,true);h.hide(true);h.w.dispatchEvent(new h.w.Event('pagehide'));await settle();assert.equal(h.d.querySelector('#start').disabled,true,'duplicate end boundaries cannot permit Start before hangup');h.d.querySelector('#start').click();await settle();assert.equal(h.actions('start').length,1);assert.equal(h.actions('stop').length,1);assert.equal(h.peer.closed,1);stopAnswer.resolve({stopped});await settle();assert.equal(h.d.querySelector('#start').disabled,false);assert.match(h.d.querySelector('#evidence').textContent,new RegExp('Server hangup confirmed: '+stopped));assertSilent(h,'hangup completion remains silent');assert.equal(h.actions('stop').length,1);
});

test('zero gathered network routes are rejected before a provider allocation',async t=>{
  const h=fixture(t,{emptyRoute:true});await h.start();assert.equal(h.actions('capabilities').length,1);assert.equal(h.actions('start').length,0);assert.equal(h.actions('stop').length,0);assert.equal(h.peer.closed,1);assert.equal(h.contexts.at(-1).closed,1);assert.equal(h.d.querySelector('#start').disabled,false);assertSilent(h,'there is no native media transport');
});

test('a drained response cannot reopen speech in the quiet interval before automatic hangup',async t=>{
  const h=await speaking(t),before=h.facts().positiveSignalSamples;h.emit({type:'output_audio_buffer.stopped',response_id:'native-response'});assertSilent(h,'native buffer drained');h.buffer();h.advance(500);assertSilent(h,'late duplicate started event is retired');assert.equal(h.facts().positiveSignalSamples,before);assert.equal(h.guide.snapshot().state,'idle');assert.equal(h.d.querySelector('#fixture-tray').hidden,true);assert.equal(h.actions('stop').length,0);h.advance(400);await settle();assert.equal(h.actions('stop').length,1);assert.equal(h.actions('start').length,1);
});

test('the bounded greeting deadline closes media and retains only bounded observed poses',async t=>{
  const h=await speaking(t);h.advance(20500);await settle();assertSilent(h,'the authored connection check has ended');assert.equal(h.actions('start').length,1);assert.equal(h.actions('stop').length,1);assert.equal(h.peer.closed,1);assert.equal(h.contexts.at(-1).closed,1);assert.ok(h.facts().observations.length<=16);assert.equal(h.guide.snapshot().state,'idle');assert.equal(h.d.querySelector('#start').disabled,false);
});

test('srcObject clock reset cannot freeze fresh measured phrase progress after stream replacement',async t=>{
  const h=await speaking(t,{resetOnAttach:true});h.audio.currentTime=10;h.frame(0);const previousClock=h.planner.snapshot().clockMs;h.attach({peak:.1,bin:40});assert.equal(h.audio.currentTime,0,'native media replacement reset the element clock');h.advance(250);assert.ok(h.facts().speechSignal.amplitude>0);assert.ok(h.levels.at(-1).outputTimeMs>0,'fresh replacement media is not clamped under the old origin');assert.ok(h.planner.snapshot().clockMs>previousClock,'current phrase progress resumes with admitted actual media');assert.equal(h.actions('start').length,1);assert.equal(h.actions('stop').length,0);
});

test('a failed avatar asset may retry only after a new intentional Start',async t=>{
  const h=fixture(t,{loadResults:['error','load']});await h.start();assert.equal(h.guides.length,0);assert.equal(h.requests.length,0);assert.equal(h.contexts[0].closed,1);assert.equal(h.d.querySelector('#start').disabled,false);h.advance(1000);assert.equal(h.requests.length,0,'the failure does not retry or allocate automatically');assert.equal(h.contexts.length,1);
  await h.start();assert.equal(h.guides.length,1);assert.equal(h.contexts.length,2);assert.equal(h.actions('capabilities').length,1);assert.equal(h.actions('start').length,1);h.bind();h.attach();h.buffer();h.advance(150);assert.equal(h.facts().speechSignal.valid,true);assert.equal(h.guide.snapshot().speechVisual.rippleActive,true);assert.equal(h.actions('stop').length,0);
});

test('a superseded stream play rejection cannot hang up the successful current native stream',async t=>{
  const oldPlay=deferred(),h=fixture(t,{playResults:[oldPlay]});await h.start();h.bind();h.attach();h.buffer();h.advance(100);h.attach({peak:.1,bin:40});h.advance(150);assert.ok(h.facts().speechSignal.bands[4]>0);oldPlay.reject(Error('Synthetic aborted playback for a replaced srcObject'));await settle();h.frame();assert.equal(h.actions('stop').length,0,'an obsolete play promise does not own the current stream');assert.equal(h.peer.closed,0);assert.equal(h.guide.snapshot().state,'speaking');assert.ok(h.facts().speechSignal.bands[4]>0);assert.equal(h.guide.snapshot().speechVisual.rippleActive,true);assert.equal(h.actions('start').length,1);
});
