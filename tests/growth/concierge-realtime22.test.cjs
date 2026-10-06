'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const server=require('../../netlify/functions/_britesConciergeVoice.js'),client=require('../../brites-concierge-voice.js');
const SDP='v=0\r\nm=audio 9 UDP/TLS/RTP/SAVPF 111\r\n';
const env={BRITES_GROWTH_SANDBOX:'1',BRITES_GROWTH_NAMESPACE:'Brites_Growth_Sandbox',BRITES_CONCIERGE_REALTIME_ENABLED:'1',BRITES_CONCIERGE_REALTIME_PUBLIC_DEMO:'1',BRITES_CONCIERGE_REALTIME_DEMO_USD_CAP:'10',BRITES_CONCIERGE_REALTIME_RESERVE_USD:'1',OPENAI_API_KEY:'fixture-only-provider-key'};
function request(body,{origin='https://preview.test',operator=false}={}){return new Request('https://preview.test/api/concierge-voice',{method:'POST',headers:{...(origin?{Origin:origin}:{}),'Content-Type':'application/json',...(operator?{'X-Growth-Key':'fixture-admin'}:{})},body:JSON.stringify(body)});}
function fixture(changes={}){
  const rows=new Map(),requests=[],rates=[],deadlines=[];let at=1000000;const control={enabled:true,aiEnabled:false,aiDailyUsdCap:0};
  const col=suffix=>({firestore:db,doc(id){const key=suffix+'/'+id;return {key,get:async()=>({exists:rows.has(key),data:()=>rows.get(key)}),set:async data=>rows.set(key,data)};}});
  let serial=Promise.resolve();const db={runTransaction(task){const next=serial.then(()=>task({get:async ref=>ref.get(),set(ref,data){rows.set(ref.key,data);}}));serial=next.catch(()=>{});return next;}};
  const service={setup:async()=>control,col,rateLimit:async(key,limit)=>{rates.push({key,limit});return true;}};
  const fetch=async(url,init)=>{requests.push({url,init});if(url.includes('/v1/models/'))return Response.json({id:'gpt-realtime-2.1'});if(url.endsWith('/hangup'))return new Response(null,{status:200});return new Response(SDP,{status:201,headers:{Location:'/v1/realtime/calls/rtc_fixture'}});};
  const handler=server.createHandler({env,service,authorize:async req=>req.headers.get('X-Growth-Key')==='fixture-admin',fetch,scheduleHangup:async row=>{deadlines.push(row);return true;},now:()=>at,...changes});
  return {rows,requests,rates,deadlines,service,control,handler,advance(ms){at+=ms;},now:()=>at};
}
async function token(f){const r=await f.handler(request({action:'capabilities'}));assert.equal(r.status,200);return (await r.json()).demoToken;}

test('explicit isolated demo grants native opt-in without activating bulk inference',async()=>{
  const f=fixture();const demoToken=await token(f);assert.equal(f.control.aiEnabled,false);assert.equal(f.requests.length,0);assert.equal(f.rows.size,0);
  const r=await f.handler(request({action:'start',sdp:SDP,demoToken})),result=await r.json();assert.equal(r.status,200);assert.equal(result.maxDurationMs,120000);assert.equal(f.control.aiEnabled,false);assert.equal(f.rows.get('VoiceUsage/preview-budget').reservedCents,100);assert.equal(f.rows.has('Usage/1970-01-01'),false);assert.equal(f.deadlines.length,1);assert.equal(JSON.stringify(result).includes(env.OPENAI_API_KEY),false);
  const config=JSON.parse(f.requests[0].init.body.get('session'));assert.equal(config.tool_choice,'auto');assert.equal(config.output_modalities[0],'audio');assert.equal(config.audio.output.voice,'marin');assert.match(config.instructions,/Before naming or recommending/);
});

test('same-origin guest capability is unusable on live namespace, cross-origin, or without origin',async()=>{
  for(const changes of [{env:{...env,BRITES_GROWTH_NAMESPACE:'Live'}},{env:{...env,BRITES_CONCIERGE_REALTIME_PUBLIC_DEMO:'0'}}]){const f=fixture(changes);const response=await f.handler(request({action:'capabilities'}));assert.ok([200,401,503].includes(response.status));if(response.status===200)assert.equal((await response.json()).enabled,false);assert.equal(f.requests.length,0);}
  const f=fixture();for(const origin of ['https://other.test',null]){const response=await f.handler(request({action:'capabilities'},{origin}));assert.ok([401,403].includes(response.status));}assert.equal(f.requests.length,0);
});

test('guest token expires, cannot be forged or reused, and provider failures keep allocations held',async()=>{
  const f=fixture({fetch:async()=>Response.json({error:{code:'insufficient_quota',message:'private provider detail'}},{status:429})});const first=await token(f);
  assert.equal((await f.handler(request({action:'start',sdp:SDP,demoToken:first+'x'}))).status,401);assert.equal(f.rows.size,0);
  const failed=await f.handler(request({action:'start',sdp:SDP,demoToken:first}));assert.equal(failed.status,503);assert.doesNotMatch(await failed.text(),/quota|private provider detail|fixture-only/);assert.equal(f.rows.get('VoiceUsage/preview-budget').reservedCents,100);
  const reused=await f.handler(request({action:'start',sdp:SDP,demoToken:first}));assert.equal(reused.status,401);assert.equal((await reused.json()).code,'VOICE_SESSION_REUSED');assert.equal(f.rows.get('VoiceUsage/preview-budget').reservedCents,100);
  const second=await token(f);f.advance(600001);assert.equal((await f.handler(request({action:'start',sdp:SDP,demoToken:second}))).status,401);assert.equal(f.rows.get('VoiceUsage/preview-budget').calls,1);
  assert.deepEqual(f.rows.get('VoiceDiagnostics/last-start'),{at:1000000,stage:'provider',providerStatus:429,code:'insufficient_quota'});
});

test('dedicated reservations serialize concurrent starts, never reset overnight, and never touch bulk Usage',async()=>{
  const f=fixture(),reserve=server.createDemoBudgetReservation(f.service,{capUsd:1,now:f.now});
  const results=await Promise.all([reserve(1,'session-one-unique'),reserve(1,'session-two-unique')]);assert.equal(results.filter(Boolean).length,1);assert.equal(f.rows.get('VoiceUsage/preview-budget').reservedCents,100);
  f.advance(24*60*60000);assert.equal(await reserve(1,'session-three-unique'),null);assert.equal(f.rows.get('VoiceUsage/preview-budget').reservedCents,100);assert.equal([...f.rows.keys()].some(key=>key.startsWith('Usage/')),false);
  f.control.enabled=false;assert.equal(await server.createDemoBudgetReservation(f.service,{capUsd:10})(1,'another-session-unique'),null);
});

test('allocation and rate checks fail before provider or microphone can start',async()=>{
  const f=fixture();f.rows.set('VoiceUsage/preview-budget',{reservedCents:1000,spentCents:0,calls:10});const r=await f.handler(request({action:'capabilities'}));assert.equal(r.status,429);assert.equal(f.requests.length,0);
  const f2=fixture();const demoToken=await token(f2);f2.service.rateLimit=async()=>false;assert.equal((await f2.handler(request({action:'start',sdp:SDP,demoToken}))).status,429);assert.equal(f2.rows.size,0);assert.equal(f2.requests.length,0);
});

test('operator readiness verifies account model access without inference and keeps diagnostics private',async()=>{
  const f=fixture();assert.equal((await f.handler(request({action:'readiness'}))).status,401);assert.equal(f.requests.length,0);
  const r=await f.handler(request({action:'readiness'},{operator:true})),body=await r.json();assert.equal(r.status,200);assert.equal(body.ready,true);assert.equal(body.inference,false);assert.equal(body.model,'gpt-realtime-2.1');assert.equal(f.rows.size,0);assert.equal(f.requests.length,1);assert.equal(f.requests[0].init.method,undefined);assert.equal(f.requests[0].url,'https://api.openai.com/v1/models/gpt-realtime-2.1');assert.doesNotMatch(JSON.stringify(body),/fixture-only-provider-key/);
  const denied=fixture({fetch:async()=>Response.json({error:{code:'invalid_api_key',message:'account xyz'}},{status:401})});const result=await (await denied.handler(request({action:'readiness'},{operator:true}))).json();assert.equal(result.providerStatus,401);assert.equal(result.providerCode,'invalid_api_key');assert.doesNotMatch(JSON.stringify(result),/account xyz/);
});

test('invalid SDP answer hangs up a verifiable provider call and exposes no account diagnostics to guests',async()=>{
  const calls=[],f=fixture({fetch:async(url,init)=>{calls.push(url);return url.endsWith('/hangup')?new Response(null,{status:200}):new Response('not an SDP',{status:201,headers:{Location:'/v1/realtime/calls/rtc_fixture'}});}});const demoToken=await token(f);const r=await f.handler(request({action:'start',sdp:SDP,demoToken}));assert.equal(r.status,503);assert.equal(calls.at(-1),'https://api.openai.com/v1/realtime/calls/rtc_fixture/hangup');assert.equal(f.deadlines.length,0);assert.equal(f.rows.get('VoiceUsage/preview-budget').reservedCents,100);assert.equal(f.rows.get('VoiceDiagnostics/last-start').stage,'verification');
});

function nativeFixture({greeting=true,iceGathering='complete',onTool=async()=>({verified:true,products:[]})}={}){
  let channel,peer,microphones=0,stopped=0,at=0;const audioNodes=[],states=[],transcripts=[],errors=[],sent=[],requests=[],levels=[],timers=new Map(),listeners=new Map(),frames=[];let timerId=0;
  const track={stop(){stopped++;}},stream={getTracks:()=>[track],getAudioTracks:()=>[track]};
  class Peer{constructor(){peer=this;this.connectionState='new';this.iceGatheringState=iceGathering;this.listeners=new Map();}addEventListener(name,fn){this.listeners.set(name,fn);}removeEventListener(name){this.listeners.delete(name);}completeIce(){this.iceGatheringState='complete';this.listeners.get('icegatheringstatechange')?.();}addTrack(){}createDataChannel(){channel={readyState:'connecting',send(raw){sent.push(JSON.parse(raw));},close(){this.readyState='closed';}};return channel;}createOffer(){return Promise.resolve({type:'offer',sdp:SDP});}setLocalDescription(offer){this.localDescription=offer;return Promise.resolve();}setRemoteDescription(answer){this.remoteDescription=answer;channel.readyState='open';channel.onopen?.();return Promise.resolve();}close(){this.closed=true;}}
  const document={hidden:false,body:{appendChild(){}},createElement(){const audio={setAttribute(){},play:async()=>{},pause(){this.paused=true;},remove(){this.removed=true;},srcObject:stream};audioNodes.push(audio);return audio;},addEventListener(name,fn){listeners.set(name,fn);},removeEventListener(name){listeners.delete(name);}};
  class AudioContext{resume(){return Promise.resolve();}close(){return Promise.resolve();}createMediaStreamSource(){return {connect(){},disconnect(){}};}createAnalyser(){return {fftSize:512,getFloatTimeDomainData(samples){samples.fill(.1);},disconnect(){}};}}
  const runtime={location:{origin:'https://preview.test'},document,navigator:{mediaDevices:{getUserMedia:async()=>{microphones++;return stream;}}},RTCPeerConnection:Peer,AudioContext,AbortController,setTimeout(fn,ms){const id=++timerId;timers.set(id,{fn,at:at+ms});return id;},clearTimeout(id){timers.delete(id);},requestAnimationFrame(fn){frames.push(fn);return frames.length;},cancelAnimationFrame(){},addEventListener(name,fn){listeners.set(name,fn);},removeEventListener(name){listeners.delete(name);},fetch:async(url,init)=>{const body=JSON.parse(init.body);requests.push(body);if(body.action==='capabilities')return Response.json({enabled:true,demoToken:'signed-demo-fixture'});if(body.action==='start')return Response.json({sdp:SDP,stopToken:'signed-stop-fixture',maxDurationMs:120000});return Response.json({stopped:true});}};
  const voice=client.create({runtime,greeting,onState:value=>states.push(value),onLevel:value=>levels.push(value),onTranscript:value=>transcripts.push(value),onError:value=>errors.push(value),onTool});
  return {runtime,voice,states,transcripts,errors,sent,requests,levels,audioNodes,timers,listeners,frames,counts:()=>({microphones,stopped}),peer:()=>peer,channel:()=>channel,emit:event=>channel.onmessage?.({data:JSON.stringify(event)}),advance(ms){at+=ms;for(const [id,timer]of [...timers])if(timer.at<=at){timers.delete(id);timer.fn();}}};
}
const flush=()=>new Promise(resolve=>setImmediate(resolve));

test('native greeting, token forwarding, streamed captions and audio-buffer state stay synchronized',async()=>{
  const f=nativeFixture();assert.equal(f.requests.length,0);assert.equal(f.counts().microphones,0);assert.equal(await f.voice.start(),true);assert.equal(f.requests.find(x=>x.action==='start').demoToken,'signed-demo-fixture');assert.equal(f.voice.state,'thinking');assert.match(f.sent[0].response.instructions,/Greet the shopper warmly/);
  f.emit({type:'response.output_audio_transcript.delta',item_id:'item-1',delta:'Hello, '});f.emit({type:'response.output_audio_transcript.delta',item_id:'item-1',delta:'how can I help?'});assert.equal(f.transcripts.length,2);assert.equal(f.transcripts[0].itemId,'item-1');assert.equal(f.transcripts[0].final,false);
  f.emit({type:'output_audio_buffer.started'});assert.equal(f.voice.state,'speaking');f.emit({type:'response.done',response:{status:'completed'}});assert.equal(f.voice.state,'speaking');f.emit({type:'response.output_audio_transcript.done',item_id:'item-1',transcript:'Hello, how can I help?'});assert.deepEqual(f.transcripts.at(-1),{role:'assistant',text:'Hello, how can I help?',final:true,itemId:'item-1'});f.emit({type:'output_audio_buffer.stopped'});assert.equal(f.voice.state,'listening');await f.voice.dispose();assert.equal(f.timers.size,0);assert.equal(f.listeners.size,0);
});

test('native interruption clears buffered voice and never starts a stale catalogue answer over a new turn',async()=>{
  let finish;const f=nativeFixture({greeting:false,onTool:()=>new Promise(resolve=>{finish=resolve;})});await f.voice.start();f.emit({type:'response.function_call_arguments.done',name:'find_jewellery',call_id:'lookup1',arguments:'{"message":"A graduation gift"}'});assert.equal(f.voice.state,'thinking');f.emit({type:'response.done'});assert.equal(f.voice.state,'thinking');
  f.emit({type:'input_audio_buffer.speech_started'});assert.equal(f.voice.state,'listening');assert.ok(f.sent.some(x=>x.type==='output_audio_buffer.clear'));finish({verified:true,products:[]});await flush();assert.ok(f.sent.some(x=>x.item?.call_id==='lookup1'));assert.equal(f.sent.some(x=>x.type==='response.create'),false);await f.voice.dispose();assert.ok(f.counts().stopped>0);
});

test('native deadline, provider failure, hidden page and media refusal close resources without synthetic voice',async()=>{
  const f=nativeFixture({greeting:false});await f.voice.start();f.advance(120000);await flush();assert.equal(f.voice.state,'idle');assert.ok(f.counts().stopped>0);assert.equal(f.peer().closed,true);assert.equal(f.requests.at(-1).action,'stop');await f.voice.dispose();
  const denied=nativeFixture();denied.runtime.navigator.mediaDevices.getUserMedia=async()=>{throw Error('Microphone denied.');};assert.equal(await denied.voice.start(),false);assert.equal(denied.requests.some(x=>x.action==='start'),false);assert.equal(denied.voice.state,'idle');assert.equal(denied.errors[0],'Your microphone could not be opened. You can still type.');await denied.voice.dispose();
  const hidden=nativeFixture({greeting:false});await hidden.voice.start();hidden.runtime.document.hidden=true;hidden.listeners.get('visibilitychange')();await flush();assert.equal(hidden.voice.state,'idle');assert.ok(hidden.counts().stopped>0);await hidden.voice.dispose();
});


test('End voice silences and closes local media immediately even when server hangup has not returned',async()=>{
  const f=nativeFixture({greeting:false});await f.voice.start();const original=f.runtime.fetch;let release;
  f.runtime.fetch=async(url,init)=>JSON.parse(init.body).action==='stop'?new Promise(resolve=>{release=resolve;}):original(url,init);
  const stopping=f.voice.stop();assert.equal(f.voice.state,'closing');assert.ok(f.counts().stopped>0);assert.equal(f.audioNodes[0].paused,true);assert.equal(f.audioNodes[0].srcObject,null);assert.equal(f.audioNodes[0].removed,true);assert.equal(f.peer().closed,true);assert.equal(f.channel().readyState,'closed');assert.equal(typeof release,'function');
  release(Response.json({stopped:true}));await stopping;assert.equal(f.voice.state,'idle');await f.voice.dispose();assert.equal(f.timers.size,0);
});

test('a canceled start with a late signed provider answer immediately requests its exact hangup',async()=>{
  const f=nativeFixture({greeting:false}),original=f.runtime.fetch;let release;
  f.runtime.fetch=async(url,init)=>{const body=JSON.parse(init.body);if(body.action==='start'){f.requests.push(body);return new Promise(resolve=>{release=resolve;});}return original(url,init);};
  const starting=f.voice.start();await flush();assert.equal(typeof release,'function');await f.voice.stop();assert.equal(await starting,false);assert.equal(f.peer().closed,true);
  release(Response.json({sdp:SDP,stopToken:'late-signed-token',maxDurationMs:120000}));await flush();assert.deepEqual(f.requests.at(-1),{action:'stop',stopToken:'late-signed-token'});assert.equal(f.voice.state,'idle');assert.equal(f.timers.size,0);await f.voice.dispose();
});

test('per-tool signal aborts on manual interruption and lookup deadline before stale UI can commit',async()=>{
  let signal,resolve;const f=nativeFixture({greeting:false,onTool:(args,options)=>{signal=options.signal;return new Promise(done=>{resolve=done;});}});await f.voice.start();f.emit({type:'response.function_call_arguments.done',name:'find_jewellery',call_id:'lookup-signal',arguments:'{"message":"A gift"}'});assert.equal(signal.aborted,false);f.voice.interrupt();assert.equal(signal.aborted,true);resolve({verified:true,products:[]});await flush();assert.equal(f.sent.some(value=>value.type==='response.create'),false);await f.voice.dispose();
  let timeoutSignal;const timed=nativeFixture({greeting:false,onTool:(args,options)=>{timeoutSignal=options.signal;return new Promise(()=>{});}});await timed.voice.start();timed.emit({type:'response.function_call_arguments.done',name:'find_jewellery',call_id:'lookup-timeout',arguments:'{"message":"A gift"}'});timed.advance(14000);await flush();assert.equal(timeoutSignal.aborted,true);assert.ok(timed.sent.some(value=>value.item?.output?.includes('could not be checked')));await timed.voice.dispose();assert.equal(timed.timers.size,0);
});


test('a complete ICE offer proceeds immediately without a temporary gathering listener',async()=>{
  const f=nativeFixture({greeting:false});assert.equal(await f.voice.start(),true);assert.equal(f.peer().listeners.size,0);assert.equal(f.requests.filter(value=>value.action==='start').length,1);await f.voice.dispose();
});

test('one-shot SDP negotiation waits for delayed ICE completion before a provider start',async()=>{
  const f=nativeFixture({greeting:false,iceGathering:'gathering'}),starting=f.voice.start();await flush();assert.equal(f.voice.state,'connecting');assert.equal(f.requests.some(value=>value.action==='start'),false);assert.equal(f.peer().listeners.has('icegatheringstatechange'),true);
  f.peer().localDescription.sdp=SDP+'a=end-of-candidates\r\n';f.peer().completeIce();assert.equal(await starting,true);assert.equal(f.requests.filter(value=>value.action==='start').length,1);assert.equal(f.requests.find(value=>value.action==='start').sdp,SDP+'a=end-of-candidates\r\n');assert.equal(f.peer().listeners.size,0);await f.voice.dispose();assert.equal(f.timers.size,0);
});

test('canceling while ICE gathers removes listeners, closes microphone and never starts a provider call',async()=>{
  const f=nativeFixture({greeting:false,iceGathering:'gathering'}),starting=f.voice.start();await flush();assert.equal(f.peer().listeners.size,1);await f.voice.stop();assert.equal(await starting,false);assert.equal(f.peer().listeners.size,0);assert.equal(f.peer().closed,true);assert.ok(f.counts().stopped>0);assert.equal(f.requests.some(value=>value.action==='start'),false);f.peer().completeIce();await flush();assert.equal(f.requests.some(value=>value.action==='start'),false);await f.voice.dispose();assert.equal(f.timers.size,0);
});

test('ICE gathering timeout cleans all local resources and reports a bounded network setup error',async()=>{
  const f=nativeFixture({greeting:false,iceGathering:'gathering'}),starting=f.voice.start();await flush();f.advance(10000);assert.equal(await starting,false);assert.equal(f.voice.state,'idle');assert.equal(f.peer().listeners.size,0);assert.equal(f.peer().closed,true);assert.ok(f.counts().stopped>0);assert.equal(f.requests.some(value=>value.action==='start'),false);assert.equal(f.errors[0],'OpenAI voice could not finish network setup in this browser. You can retry or type here.');await f.voice.dispose();assert.equal(f.timers.size,0);
});


test('known microphone browser failures show only fixed safe guidance and never start a paid session',async()=>{
  for(const [name,expected]of [
    ['NotAllowedError','Microphone permission was not granted. Allow microphone access in your browser, or type here.'],
    ['NotFoundError','No microphone was found. Connect or enable a microphone, or type here.'],
    ['NotReadableError','Your microphone is busy or unavailable. Close other apps using it, or type here.']
  ]){
    const f=nativeFixture();f.runtime.navigator.mediaDevices.getUserMedia=async()=>{throw Object.assign(Error('private device serial and account detail'),{name});};assert.equal(await f.voice.start(),false);assert.equal(f.voice.state,'idle');assert.equal(f.requests.some(value=>value.action==='start'),false);assert.equal(f.errors[0],expected);assert.doesNotMatch(JSON.stringify(f.errors),/private device|account detail/);assert.equal(f.audioNodes.length,0);await f.voice.dispose();assert.equal(f.timers.size,0);
  }
});

test('unknown microphone exceptions and provider response text cannot disclose private details',async()=>{
  const mic=nativeFixture();mic.runtime.navigator.mediaDevices.getUserMedia=()=>{throw Error('secret device and account detail');};assert.equal(await mic.voice.start(),false);assert.equal(mic.errors[0],'Your microphone could not be opened. You can still type.');await mic.voice.dispose();
  const provider=nativeFixture();provider.runtime.fetch=async()=>Response.json({enabled:false,code:'unrecognized_private_code',message:'secret account billing details'},{status:403});assert.equal(await provider.voice.start(),false);assert.equal(provider.errors[0],'OpenAI voice could not connect. You can still type.');assert.equal(provider.counts().microphones,0);assert.doesNotMatch(JSON.stringify([...mic.errors,...provider.errors]),/secret|account|billing/);await provider.voice.dispose();
});

test('a media connection timeout uses fixed guidance and cleans remote media before signed hangup',async()=>{
  const f=nativeFixture({greeting:false});let awaitingMedia;
  const OriginalPeer=f.runtime.RTCPeerConnection;
  f.runtime.RTCPeerConnection=class extends OriginalPeer{setRemoteDescription(answer){this.remoteDescription=answer;awaitingMedia=true;return Promise.resolve();}};
  const starting=f.voice.start();await flush();assert.equal(awaitingMedia,true);f.advance(10000);assert.equal(await starting,false);assert.equal(f.errors[0],'OpenAI voice could not establish a media connection in this browser. You can retry or type here.');assert.equal(f.voice.state,'idle');assert.ok(f.counts().stopped>0);assert.equal(f.peer().closed,true);assert.equal(f.audioNodes[0].paused,true);assert.equal(f.requests.at(-1).action,'stop');await f.voice.dispose();assert.equal(f.timers.size,0);
});

test('peer failure reports fixed connection guidance before ending rather than only an idle state',async()=>{
  const f=nativeFixture({greeting:false});await f.voice.start();f.peer().connectionState='failed';f.peer().onconnectionstatechange();assert.equal(f.errors.at(-1),'OpenAI voice could not establish a media connection in this browser. You can retry or type here.');assert.equal(f.peer().closed,true);await flush();assert.equal(f.voice.state,'idle');await f.voice.dispose();
});
