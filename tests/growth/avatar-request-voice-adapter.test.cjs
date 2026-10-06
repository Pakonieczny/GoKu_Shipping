'use strict';
// Real adapter with deterministic browser/media doubles. No microphone,
// provider, browser permission or physically audible playback is tested here.
const test=require('node:test'),assert=require('node:assert/strict');
const adapter=require('../../brites-concierge-voice.js');
const SDP='v=0\r\nm=audio 9 UDP/TLS/RTP/SAVPF 111\r\n';
const tick=()=>new Promise(resolve=>setImmediate(resolve));
function deferred(){let resolve,reject;const promise=new Promise((a,b)=>{resolve=a;reject=b;});return {promise,resolve,reject};}
function fixture(t,options={}){
  let at=0,serial=0,capabilities=options.capabilities||Response.json({enabled:true}),gum=options.gum,playMode='resolve',resumeMode=options.resumeMode||'resolve',peer,channel;
  const timers=new Map(),requests=[],tracks=[],audioNodes=[],errors=[],stopped=[],phases=[],blocked=[],resumed=[],states=[],listeners=new Map(),contexts=[];
  const timer=(fn,ms)=>{const id=++serial;timers.set(id,{fn,at:at+ms});return id;},clear=id=>timers.delete(id);
  const stream=()=>{const track={stops:0,stop(){this.stops++;}},value={getTracks:()=>[track],getAudioTracks:()=>[track]};tracks.push(track);return value;};
  const doc={hidden:false,body:{appendChild(){}},createElement(){const audio={srcObject:null,plays:0,paused:0,removed:false,setAttribute(){},pause(){this.paused++;},remove(){this.removed=true;},play(){this.plays++;if(playMode==='reject')return Promise.reject(Error('Private autoplay diagnostic'));if(playMode?.promise)return playMode.promise;return Promise.resolve();}};audioNodes.push(audio);return audio;},addEventListener(k,f){listeners.set(k,f);},removeEventListener(k){listeners.delete(k);}};
  class Peer{
    constructor(){peer=this;this.iceGatheringState='complete';this.connectionState='new';}
    addTrack(){}createDataChannel(){channel={readyState:'connecting',send(){},close(){this.readyState='closed';}};return channel;}
    async createOffer(){return {type:'offer',sdp:SDP};}async setLocalDescription(value){this.localDescription=value;}
    async setRemoteDescription(){queueMicrotask(()=>{channel.readyState='open';channel.onopen?.();});}
    close(){this.closed=true;}
  }
  class AudioContext{
    constructor(){contexts.push(this);}resume(){if(resumeMode==='throw')throw Error('Private meter issue');if(resumeMode==='reject')return Promise.reject(Error('Private meter issue'));if(resumeMode==='pending')return new Promise(()=>{});return Promise.resolve();}
    close(){this.closed=true;return Promise.resolve();}createMediaStreamSource(){return {connect(){},disconnect(){}};}
    createAnalyser(){return {fftSize:512,disconnect(){},getFloatTimeDomainData(samples){samples.fill(0);}};}
  }
  let micCalls=0;
  const runtime={document:doc,location:{origin:'https://isolated.example'},navigator:{mediaDevices:{getUserMedia(){micCalls++;return gum?gum():Promise.resolve(stream());}}},RTCPeerConnection:Peer,...(options.noMeter?{}:{AudioContext}),AbortController,setTimeout:timer,clearTimeout:clear,requestAnimationFrame:()=>++serial,cancelAnimationFrame(){},addEventListener(k,f){listeners.set(k,f);},removeEventListener(k){listeners.delete(k);},fetch:async(url,init)=>{const value=JSON.parse(init.body);requests.push(value);if(value.action==='capabilities')return capabilities?.promise?await capabilities.promise:capabilities.clone();if(value.action==='start')return Response.json({sdp:SDP,stopToken:'test-only-token',maxDurationMs:120000});return Response.json({stopped:true});}};
  const voice=adapter.create({runtime,greeting:false,...(options.headers?{headers:options.headers}:{}),onState:value=>states.push(value),onError:(message,detail)=>errors.push({message,detail}),onStopped:(reason,detail)=>stopped.push({reason,detail}),onConnectionPhase:value=>phases.push(value),...(options.noRecovery?{}:{onPlaybackBlocked:(message,detail)=>blocked.push({message,detail}),onPlaybackResumed:()=>resumed.push(true)})});
  t.after(()=>voice.dispose());
  return {voice,runtime,requests,tracks,audioNodes,errors,stopped,phases,blocked,resumed,states,contexts,listeners,stream,peer:()=>peer,micCalls:()=>micCalls,setCapabilities:value=>capabilities=value,setGum:value=>gum=value,setPlay:value=>playMode=value,setResume:value=>resumeMode=value,track:()=>peer.ontrack({streams:[stream()]}),async advance(ms){at+=ms;for(const [id,value] of [...timers])if(value.at<=at){timers.delete(id);value.fn();}await tick();}};
}
test('capability, start and stop transports reject redirects with an operator header',async t=>{
  const f=fixture(t,{headers:()=>({'X-Growth-Key':'test-only-operator-key'})}),fetch=f.runtime.fetch,transports=[];
  f.runtime.fetch=async(url,init)=>{transports.push(init);return fetch(url,init);};
  assert.equal(await f.voice.start(),true);await f.voice.stop('user');
  assert.deepEqual(f.requests.map(value=>value.action),['capabilities','start','stop']);
  assert.equal(transports.length,3);
  for(const init of transports){assert.equal(init.redirect,'error');assert.equal(init.credentials,'same-origin');assert.equal(init.headers['X-Growth-Key'],'test-only-operator-key');}
});
test('redirect transport rejection creates no microphone and exposes no private failure',async t=>{
  const f=fixture(t,{headers:()=>({'X-Growth-Key':'test-only-operator-key'})});let calls=0;
  f.runtime.fetch=async(url,init)=>{calls++;assert.equal(init.redirect,'error');throw TypeError('Private redirect diagnostic and destination');};
  assert.equal(await f.voice.start(),false);assert.equal(calls,1);assert.equal(f.micCalls(),0);assert.equal(f.peer(),undefined);
  assert.equal(f.voice.state,'idle');assert.equal(f.voice.lastError.code,'VOICE_UNAVAILABLE');assert.equal(f.errors.length,1);
  assert.doesNotMatch(JSON.stringify(f.errors),/Private|redirect diagnostic|destination|operator-key/);
});
for(const body of ['json','html'])test('401 '+body+' produces stable sign-in failure before microphone permission',async t=>{
  const capabilities=body==='json'?Response.json({code:'PREVIEW_SIGN_IN_REQUIRED',error:'Private upstream detail'},{status:401}):new Response('<html>Private upstream detail</html>',{status:401});
  const f=fixture(t,{capabilities});assert.equal(await f.voice.start(),false);assert.equal(f.voice.state,'idle');assert.equal(f.micCalls(),0);assert.equal(f.errors.length,1);
  assert.deepEqual(f.voice.lastError,{code:'PREVIEW_SIGN_IN_REQUIRED',message:adapter.MESSAGES.signIn,recovery:'sign-in',retryable:false});
  assert.equal(f.stopped[0].reason,'failed');assert.equal(f.stopped[0].detail.error,f.voice.lastError);assert.equal(f.requests.length,1);assert.doesNotMatch(JSON.stringify(f.errors),/Private|upstream/);
});
test('reused voice token offers a fresh-session retry, cleans up and waits for another explicit Talk action',async t=>{
  const f=fixture(t,{capabilities:Response.json({enabled:true,demoToken:'synthetic-first-session'})}),fetch=f.runtime.fetch;let rejected=false;
  f.runtime.fetch=async(url,init)=>{const body=JSON.parse(init.body);if(body.action==='start'&&!rejected){rejected=true;f.requests.push(body);return Response.json({enabled:false,code:'VOICE_SESSION_REUSED',message:'Private allocation detail'},{status:401});}return fetch(url,init);};
  assert.equal(await f.voice.start(),false);assert.equal(f.voice.state,'idle');
  assert.deepEqual(f.voice.lastError,{code:'VOICE_SESSION_EXPIRED',message:adapter.MESSAGES.expired,recovery:'retry',retryable:true});
  assert.equal(f.errors.length,1);assert.equal(f.stopped[0].reason,'failed');assert.equal(f.stopped[0].detail.error,f.voice.lastError);
  assert.ok(f.tracks[0].stops>0);assert.equal(f.peer().closed,true);assert.equal(f.audioNodes[0].removed,true);assert.equal(f.contexts[0].closed,true);
  await tick();assert.deepEqual(f.requests.map(x=>x.action),['capabilities','start']);assert.equal(f.micCalls(),1);
  assert.doesNotMatch(JSON.stringify(f.errors),/Private|allocation detail|synthetic-first-session/);
  f.setCapabilities(Response.json({enabled:true,demoToken:'synthetic-fresh-session'}));
  assert.equal(await f.voice.start(),true);assert.equal(f.voice.state,'listening');assert.equal(f.voice.lastError,null);assert.equal(f.micCalls(),2);
  assert.deepEqual(f.requests.filter(x=>x.action==='start').map(x=>x.demoToken),['synthetic-first-session','synthetic-fresh-session']);
  assert.equal(f.errors.length,1);
});
for(const status of [503,401])test('known allocation guard error '+status+' stays a private retryable failure without sign-in or allocation-pause fallback',async t=>{
  const f=fixture(t,{capabilities:Response.json({enabled:false,code:'VOICE_GUARD_UNAVAILABLE',message:'Private ledger detail',error:{status:401,credential:'private'}},{status})});
  assert.equal(await f.voice.start(),false);assert.equal(f.voice.state,'idle');
  assert.deepEqual(f.voice.lastError,{code:'VOICE_UNAVAILABLE',message:adapter.MESSAGES.unavailable,recovery:'retry',retryable:true});
  assert.equal(f.micCalls(),0);assert.equal(f.peer(),undefined);assert.equal(f.audioNodes.length,0);assert.deepEqual(f.requests.map(x=>x.action),['capabilities']);
  assert.equal(f.errors.length,1);assert.doesNotMatch(JSON.stringify(f.errors),/Private|ledger|credential|private/);
});
for(const code of [undefined,'VOICE_DISABLED'])test('disabled capability '+String(code)+' explains state without microphone/provider start',async t=>{
  const f=fixture(t,{capabilities:Response.json({enabled:false,...(code?{code}:{})})});assert.equal(await f.voice.start(),false);assert.equal(f.voice.lastError.code,'VOICE_DISABLED');assert.equal(f.voice.lastError.message,adapter.MESSAGES.disabled);assert.equal(f.micCalls(),0);assert.equal(f.audioNodes.length,0);assert.deepEqual(f.requests.map(x=>x.action),['capabilities']);
});
test('allocation pause retains bounded sandbox control and useful local message',async t=>{
  const f=fixture(t,{capabilities:Response.json({code:'VOICE_ALLOCATION_UNAVAILABLE'},{status:503})});assert.equal(await f.voice.start(),false);assert.equal(f.voice.lastError.code,'VOICE_ALLOCATION_UNAVAILABLE');assert.equal(f.micCalls(),0);assert.equal(f.requests.length,1);
});
test('unknown upstream error/code remains private',async t=>{
  const f=fixture(t,{capabilities:Response.json({code:'secret-account-code',error:'credential=private'},{status:503})});await f.voice.start();assert.deepEqual(f.voice.lastError,{code:'VOICE_UNAVAILABLE',message:adapter.MESSAGES.unavailable,recovery:'retry',retryable:true});assert.doesNotMatch(JSON.stringify(f.errors),/account|secret|credential|private/);
});
test('unverified successful availability response cannot open microphone or paid session',async t=>{
  const f=fixture(t,{capabilities:Response.json({code:'unverified',account:'private'})});assert.equal(await f.voice.start(),false);assert.equal(f.voice.lastError.code,'VOICE_UNAVAILABLE');assert.equal(f.micCalls(),0);assert.equal(f.requests.length,1);
});
for(const [name,code,message] of [['NotAllowedError','MIC_PERMISSION_REQUIRED','micDenied'],['NotFoundError','MIC_NOT_FOUND','micMissing'],['NotReadableError','MIC_UNAVAILABLE','micBusy'],['UnknownError','MIC_UNAVAILABLE','micUnknown']])test(name+' preserves actionable microphone failure and creates no call',async t=>{
  const f=fixture(t,{gum:()=>Promise.reject(Object.assign(Error('Private device name'),{name}))});assert.equal(await f.voice.start(),false);assert.equal(f.voice.lastError.code,code);assert.equal(f.voice.lastError.message,adapter.MESSAGES[message]);assert.deepEqual(f.requests.map(x=>x.action),['capabilities']);assert.equal(f.peer(),undefined);assert.doesNotMatch(JSON.stringify(f.errors),/Private|device name/);
});
test('microphone permission timeout settles safely and stops a late grant',async t=>{
  const permission=deferred(),f=fixture(t,{gum:()=>permission.promise});const start=f.voice.start();await tick();assert.equal(f.micCalls(),1);await f.advance(20000);assert.equal(await start,false);assert.equal(f.voice.lastError.code,'MIC_PERMISSION_TIMEOUT');assert.equal(f.voice.state,'idle');const granted=f.stream();permission.resolve(granted);await tick();assert.ok(granted.getTracks()[0].stops>0);assert.equal(f.peer(),undefined);
});
test('availability timeout creates no microphone and survives late capability response',async t=>{
  const check=deferred(),f=fixture(t,{capabilities:check});const start=f.voice.start();await tick();await f.advance(12000);assert.equal(await start,false);assert.equal(f.voice.lastError.code,'VOICE_AVAILABILITY_TIMEOUT');check.resolve(Response.json({enabled:true}));await tick();assert.equal(f.micCalls(),0);assert.equal(f.errors.length,1);assert.equal(f.voice.state,'idle');
});
test('cancel during availability neither reports failure nor opens late microphone',async t=>{
  const check=deferred(),f=fixture(t,{capabilities:check});const start=f.voice.start();await tick();await f.voice.stop('user');assert.equal(await start,false);check.resolve(Response.json({enabled:true}));await tick();assert.equal(f.micCalls(),0);assert.equal(f.errors.length,0);assert.equal(f.voice.lastError,null);assert.equal(f.stopped[0].reason,'user');assert.equal(f.stopped[0].detail.error,null);
});
test('explicit retry clears only the old failure and advances clear connection phases',async t=>{
  const f=fixture(t,{capabilities:Response.json({code:'PREVIEW_SIGN_IN_REQUIRED'},{status:401})});await f.voice.start();f.setCapabilities(Response.json({enabled:true}));assert.equal(await f.voice.start(),true);assert.equal(f.voice.lastError,null);assert.equal(f.voice.state,'listening');assert.deepEqual(f.phases,['checking','checking','microphone','connecting','connected']);assert.equal(f.micCalls(),1);assert.equal(f.requests.filter(x=>x.action==='start').length,1);
});
test('parallel Talk starts cannot duplicate microphone or provider calls',async t=>{
  const check=deferred(),f=fixture(t,{capabilities:check});const first=f.voice.start();assert.equal(await f.voice.start(),false);check.resolve(Response.json({enabled:true}));assert.equal(await first,true);assert.equal(f.micCalls(),1);assert.equal(f.requests.filter(x=>x.action==='start').length,1);
});
for(const resumeMode of ['pending','reject','throw'])test('optional meter '+resumeMode+' cannot stall native speech connection',async t=>{
  const f=fixture(t,{resumeMode});assert.equal(await f.voice.start(),true);assert.equal(f.voice.state,'listening');assert.equal(f.errors.length,0);f.track();await tick();assert.equal(f.audioNodes[0].plays,1);assert.equal(f.blocked.length,0);
});
test('native playback can connect without Web Audio meter',async t=>{
  const f=fixture(t,{noMeter:true});assert.equal(await f.voice.start(),true);f.track();await tick();assert.equal(f.voice.state,'listening');assert.equal(f.errors.length,0);assert.equal(f.contexts.length,0);
});
test('visual meter sampling errors cannot interrupt connected speech',async t=>{
  const f=fixture(t);f.runtime.AudioContext.prototype.createAnalyser=()=>({fftSize:512,disconnect(){},getFloatTimeDomainData(){throw Error('Private analyser diagnostic');}});assert.equal(await f.voice.start(),true);assert.equal(f.voice.state,'listening');assert.equal(f.errors.length,0);f.track();await tick();assert.equal(f.audioNodes[0].plays,1);
});
test('autoplay gate has explicit recovery without a hidden microphone or a second paid start',async t=>{
  const f=fixture(t);await f.voice.start();f.setPlay('reject');f.track();await tick();assert.equal(f.voice.state,'listening');assert.equal(f.voice.playbackBlocked,true);assert.equal(f.blocked.length,1);assert.equal(f.errors.length,0);assert.equal(f.voice.lastError.code,'AUDIO_PLAYBACK_BLOCKED');
  f.setPlay('resolve');const requests=f.requests.length,microphones=f.micCalls();assert.equal(await f.voice.resumeAudio(),true);assert.equal(f.voice.playbackBlocked,false);assert.equal(f.voice.lastError,null);assert.equal(f.resumed.length,1);assert.equal(f.requests.length,requests);assert.equal(f.micCalls(),microphones);assert.equal(f.audioNodes[0].plays,2);await f.voice.stop();assert.ok(f.tracks[0].stops>0);assert.equal(f.voice.state,'idle');
});
test('playback recovery stays optional even with a suspended meter',async t=>{
  const f=fixture(t,{resumeMode:'pending'});await f.voice.start();f.setPlay('reject');f.track();await tick();f.setPlay('resolve');assert.equal(await f.voice.resumeAudio(),true);assert.equal(f.voice.playbackBlocked,false);
});
test('repeated rejected playback remains explicit and never silently reconnects',async t=>{
  const f=fixture(t);await f.voice.start();f.setPlay('reject');f.track();await tick();assert.equal(await f.voice.resumeAudio(),false);assert.equal(f.voice.playbackBlocked,true);assert.equal(f.blocked.length,2);assert.equal(f.errors.length,0);assert.equal(f.requests.filter(x=>x.action==='start').length,1);
});
test('host without recovery controls stops a rejected playback session and its microphone',async t=>{
  const f=fixture(t,{noRecovery:true});await f.voice.start();f.setPlay('reject');f.track();await tick();assert.equal(f.voice.state,'idle');assert.equal(f.errors.length,1);assert.equal(f.stopped[0].reason,'playback');assert.equal(f.stopped[0].detail.error.code,'AUDIO_PLAYBACK_BLOCKED');assert.ok(f.tracks[0].stops>0);assert.equal(f.requests.at(-1).action,'stop');
});
test('late rejected audio recovery cannot corrupt a restarted session',async t=>{
  const f=fixture(t);await f.voice.start();f.track();await tick();const recovery=deferred();f.setPlay(recovery);const recovering=f.voice.resumeAudio();await f.voice.stop();f.setPlay('resolve');assert.equal(await f.voice.start(),true);recovery.reject(Error('Private stale failure'));assert.equal(await recovering,false);assert.equal(f.blocked.length,0);assert.equal(f.errors.length,0);assert.equal(f.voice.lastError,null);assert.equal(f.voice.state,'listening');
});
test('resumeAudio is inert outside an explicit active audio session',async t=>{
  const f=fixture(t);assert.equal(await f.voice.resumeAudio(),false);assert.equal(f.requests.length,0);assert.equal(f.micCalls(),0);await f.voice.start();assert.equal(await f.voice.resumeAudio(),false);await f.voice.stop();assert.equal(await f.voice.resumeAudio(),false);
});
