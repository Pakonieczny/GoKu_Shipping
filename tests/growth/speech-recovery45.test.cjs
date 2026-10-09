'use strict';
// Real native adapter, deterministic WebRTC/provider event sequences. This
// verifies interruption and continuation without provider calls or an audible
// microphone/playback claim. Shopping callbacks count actual executions.
const test=require('node:test'),assert=require('node:assert/strict');
const native=require('../../brites-concierge-voice.js');
const SDP='v=0\r\nm=audio 9 UDP/TLS/RTP/SAVPF 111\r\na=candidate:fixture 1 udp 123 192.0.2.1 49152 typ host\r\n';
const settle=async()=>{for(let n=0;n<20;n++)await Promise.resolve();};
function deferred(){let resolve;const promise=new Promise(r=>resolve=r);return {resolve,promise};}
function fixture(t,{finalized=false,tool,hangStop=false,confirmed=true}={}){
  let now=0,serial=0,peer,channel,hostCalls=0,toolCalls=0;
  const sent=[],requests=[],warnings=[],transcripts=[],playback=[],events=[],timers=new Map();
  function microphone(){const track={readyState:'live',stop(){this.readyState='ended';}};return {getTracks:()=>[track],getAudioTracks:()=>[track]};}
  const audio={paused:true,ended:false,muted:false,volume:1,currentTime:0,srcObject:null,setAttribute(){},play(){this.paused=false;return Promise.resolve();},pause(){this.paused=true;},remove(){}};
  class Peer{
    constructor(){peer=this;this.iceGatheringState='complete';this.connectionState='new';}
    addTrack(){}createDataChannel(){channel={readyState:'connecting',send:v=>sent.push(JSON.parse(v)),close(){this.readyState='closed';}};return channel;}
    async createOffer(){return {sdp:SDP};}async setLocalDescription(value){this.localDescription=value;}
    async setRemoteDescription(){this.connectionState='connected';channel.readyState='open';channel.onopen();}close(){this.connectionState='closed';}
  }
  const runtime={document:{hidden:false,body:{appendChild(){}},createElement:()=>audio,addEventListener(){},removeEventListener(){}},navigator:{mediaDevices:{getUserMedia:async()=>microphone()}},location:{origin:'https://offline.test'},RTCPeerConnection:Peer,AbortController,
    setTimeout(fn,ms){const id=++serial;timers.set(id,{at:now+ms,fn});return id;},clearTimeout:id=>timers.delete(id),addEventListener(){},removeEventListener(){},
    fetch:async(_url,init)=>{const body=JSON.parse(init.body);requests.push(body);if(body.action==='stop'&&hangStop)return new Promise(()=>{});return Response.json(body.action==='capabilities'?{enabled:true}:body.action==='start'?{sdp:SDP,stopToken:'synthetic-only',maxDurationMs:120000}:{stopped:confirmed});}};
  const voice=native.create({runtime,greeting:false,onTurnWarning:v=>warnings.push(v),onTranscript:v=>transcripts.push(v),onPlaybackState:v=>playback.push(v),
    onState:v=>events.push(['state',v]),onClose:v=>events.push(['closed',v]),onSessionExpiring:v=>events.push(['expiring',v]),onSessionExpired:v=>events.push(['expired',v]),onStopped:(reason,value)=>events.push(['stopped',reason,value]),
    ...(finalized?{onFinalizedTurn:async()=>{hostCalls++;return {handled:true,ok:true,reply:'The necklace is now in your cart.',completedActions:[{type:'add',private:'NEVER SPEAK THIS'}],postcondition:'INTERNAL PRIVATE CONDITION'};}}:{}),
    onTool:async(args,context)=>{toolCalls++;return tool?tool(args,context):{verified:true,reply:'Checked.',products:[]};}});
  const emit=event=>channel.onmessage?.({data:JSON.stringify(event)}),responses=()=>sent.filter(v=>v.type==='response.create');
  async function advance(ms){const target=now+ms;while(true){const due=[...timers].filter(([,v])=>v.at<=target).sort((a,b)=>a[1].at-b[1].at)[0];if(!due)break;now=due[1].at;timers.delete(due[0]);due[1].fn();await settle();}now=target;await settle();}
  function speech(id='input-current',text=finalized?'Add this necklace to my cart':null){emit({type:'input_audio_buffer.speech_started',item_id:id});emit({type:'input_audio_buffer.speech_stopped',item_id:id});emit({type:'input_audio_buffer.committed',item_id:id});if(text!==null)emit({type:'conversation.item.input_audio_transcription.completed',item_id:id,transcript:text});}
  function bind(id='response-current',request=responses().at(-1)){assert.ok(request);emit({type:'response.created',response:{id,metadata:request.response.metadata}});return request;}
  const output=id=>emit({type:'output_audio_buffer.started',response_id:id}),drain=id=>emit({type:'output_audio_buffer.stopped',response_id:id});
  const done=(id='response-current',status='completed',status_details)=>emit({type:'response.done',response:{id,status,...(status_details?{status_details}:{})}});
  t.after(async()=>{const ending=voice.dispose();if(hangStop)await advance(5500);await ending;assert.equal(timers.size,0);});
  return {voice,emit,speech,bind,output,drain,done,advance,responses,sent,requests,warnings,transcripts,playback,events,runtime,hostCalls:()=>hostCalls,toolCalls:()=>toolCalls,
    sameSession(){assert.equal(requests.filter(v=>v.action==='start').length,1);assert.equal(requests.filter(v=>v.action==='stop').length,0);assert.equal(peer.connectionState,'connected');}};
}

test('fast local controls finish once and wait for their generation terminal event before native response.create',async t=>{
  const f=fixture(t,{tool:async()=>({ok:true,reply:'Your cart is open.'})});await f.voice.start();f.speech();f.bind();
  f.emit({type:'response.function_call_arguments.done',response_id:'response-current',call_id:'open-cart',name:'control_storefront',arguments:'{"type":"bag"}'});await settle();
  assert.equal(f.toolCalls(),1);assert.equal(f.responses().length,1,'no overlapping default-conversation generation');assert.equal(f.sent.filter(v=>v.item?.type==='function_call_output').length,1);assert.equal(f.voice.state,'thinking');
  f.done();assert.equal(f.responses().length,2);assert.equal(f.responses()[1].response.tool_choice,'none');f.bind('spoken-cart');f.output('spoken-cart');f.done('spoken-cart');f.drain('spoken-cart');assert.equal(f.voice.state,'listening');f.sameSession();
});

test('tool result waits for already-playing audio to drain without clearing the unfinished speech',async t=>{
  const f=fixture(t);await f.voice.start();f.speech();f.bind();f.output('response-current');f.emit({type:'response.function_call_arguments.done',response_id:'response-current',call_id:'check',name:'inspect_jewellery',arguments:'{"handle":"otter-necklace"}'});await settle();
  f.done();assert.equal(f.responses().length,1);assert.equal(f.voice.state,'speaking');const clears=f.sent.filter(v=>v.type==='output_audio_buffer.clear').length;
  f.drain('response-current');assert.equal(f.responses().length,2);assert.equal(f.sent.filter(v=>v.type==='output_audio_buffer.clear').length,clears);assert.equal(f.voice.state,'thinking');f.sameSession();
});

test('a later tool completing after generation.done supersedes the earlier queued explanation exactly once',async t=>{
  let calls=0;const gate=deferred(),f=fixture(t,{tool:async()=>++calls===1?{verified:true,reply:'First checked result.',products:[]}:gate.promise});await f.voice.start();f.speech();f.bind();f.emit({type:'response.function_call_arguments.done',response_id:'response-current',call_id:'first-read',name:'inspect_jewellery',arguments:'{"handle":"otter-necklace"}'});await settle();f.emit({type:'response.function_call_arguments.done',response_id:'response-current',call_id:'second-read',name:'inspect_jewellery',arguments:'{"handle":"butterfly-necklace"}'});await settle();f.done();assert.equal(f.responses().length,1);gate.resolve({verified:true,reply:'Second checked result.',products:[]});await settle();assert.equal(f.responses().length,2);f.bind('one-followup');f.done('one-followup');assert.equal(f.responses().length,2);assert.equal(f.toolCalls(),2);f.sameSession();
});

for(const failed of [false,true])test('completed control explanation supersedes its owner\u2019s '+(failed?'transient failure':'token truncation')+' without an extra speech generation',async t=>{
  const f=fixture(t,{tool:async()=>({ok:true,reply:'Your cart is open.'})});await f.voice.start();f.speech();f.bind();f.output('response-current');f.emit({type:'response.function_call_arguments.done',response_id:'response-current',call_id:'cart-once',name:'control_storefront',arguments:'{"type":"bag"}'});await settle();f.done('response-current',failed?'failed':'incomplete',failed?{error:{type:'server_error'}}:{reason:'max_output_tokens'});f.drain('response-current');assert.equal(f.responses().length,2);assert.equal(f.responses()[1].response.tool_choice,'none');assert.match(f.responses()[1].response.instructions,/Your cart is open/);f.bind('cart-explanation');f.output('cart-explanation');f.done('cart-explanation');f.drain('cart-explanation');assert.equal(f.responses().length,2);assert.equal(f.toolCalls(),1);f.sameSession();
});

test('a real shopper interruption retires a queued result and delayed old tools never rerun actions',async t=>{
  const gate=deferred(),f=fixture(t,{tool:()=>gate.promise});await f.voice.start();f.speech('old-input');f.bind('old-response');
  f.emit({type:'response.function_call_arguments.done',response_id:'old-response',call_id:'add-once',name:'control_storefront',arguments:'{"type":"bag"}'});f.speech('fresh-input');f.bind('fresh-response');gate.resolve({ok:true,reply:'Your cart is open.'});await settle();
  f.done('old-response');assert.equal(f.responses().length,2);assert.equal(f.toolCalls(),1);f.done('fresh-response');assert.equal(f.voice.state,'listening');f.sameSession();
});

test('native token truncation receives up to two bounded speech-only tails and no website action replay',async t=>{
  const f=fixture(t);await f.voice.start();f.speech();f.bind();let active='response-current';
  for(let n=0;n<3;n++){f.output(active);f.done(active,'incomplete',{reason:'max_output_tokens'});assert.equal(f.responses().length,Math.min(n+1,3));f.drain(active);assert.equal(f.responses().length,Math.min(n+2,3));if(n<2){const tail=f.responses().at(-1).response;assert.equal(tail.tool_choice,'none');assert.equal(tail.max_output_tokens,1200);assert.match(tail.instructions,/one short sentence/);active='tail-'+n;f.bind(active);}}
  assert.equal(f.toolCalls(),0);assert.equal(f.voice.state,'listening');f.sameSession();
});

test('lost native audio drain after truncation resumes only the existing answer after bounded inactivity',async t=>{
  const f=fixture(t);await f.voice.start();f.speech();f.bind();f.output('response-current');f.done('response-current','incomplete',{reason:'max_output_tokens'});
  await f.advance(29999);assert.equal(f.responses().length,1);await f.advance(1);assert.equal(f.responses().length,2);assert.equal(f.responses()[1].response.tool_choice,'none');assert.equal(f.voice.state,'thinking');assert.equal(f.warnings.length,0);f.bind('recovered-tail');f.output('recovered-tail');
  f.output('response-current');f.drain('response-current');assert.equal(f.voice.currentOutput.responseId,'recovered-tail');f.done('recovered-tail');f.drain('recovered-tail');f.sameSession();
});

test('lost drain cannot permanently discard the spoken result of an already-completed shop control',async t=>{
  const f=fixture(t,{tool:async()=>({ok:true,reply:'Your cart is open.'})});await f.voice.start();f.speech();f.bind();f.output('response-current');f.emit({type:'response.function_call_arguments.done',response_id:'response-current',call_id:'cart-once',name:'control_storefront',arguments:'{"type":"bag"}'});await settle();f.done();assert.equal(f.responses().length,1);await f.advance(30000);assert.equal(f.responses().length,2);assert.equal(f.responses()[1].response.tool_choice,'none');assert.match(f.responses()[1].response.instructions,/Your cart is open/);assert.equal(f.toolCalls(),1);assert.equal(f.warnings.length,0);f.sameSession();
});

test('current native server_error retries the spoken explanation once without executing its control again',async t=>{
  const f=fixture(t,{tool:async()=>({ok:true,reply:'Your cart is open.'})});await f.voice.start();f.speech();f.bind();f.emit({type:'response.function_call_arguments.done',response_id:'response-current',call_id:'cart-once',name:'control_storefront',arguments:'{"type":"bag"}'});await settle();f.done();f.bind('cart-explanation');
  f.done('cart-explanation','failed',{error:{type:'server_error',message:'PRIVATE ACCOUNT ERROR'}});assert.equal(f.responses().length,3);assert.equal(f.responses()[2].response.tool_choice,'none');assert.equal(f.toolCalls(),1);assert.doesNotMatch(JSON.stringify(f.sent),/PRIVATE ACCOUNT ERROR/);
  f.bind('retry-explanation');f.done('retry-explanation','failed',{error:{type:'server_error'}});assert.equal(f.responses().length,3);assert.equal(f.voice.state,'listening');assert.equal(f.warnings.length,1);assert.equal(f.toolCalls(),1);f.sameSession();
});

for(const reason of ['content_filter','unknown'])test(reason+' incomplete speech cannot create an unsafe automatic continuation',async t=>{
  const f=fixture(t);await f.voice.start();f.speech();f.bind();f.output('response-current');f.done('response-current','incomplete',{reason});f.drain('response-current');assert.equal(f.responses().length,1);assert.equal(f.voice.state,'listening');f.sameSession();
});

test('duplicate terminal events cannot restart the old same-turn tail or cancel a newer generation',async t=>{
  const f=fixture(t);await f.voice.start();f.speech();f.bind();f.done('response-current','incomplete',{reason:'max_output_tokens'});f.bind('tail');f.done('response-current','incomplete',{reason:'max_output_tokens'});assert.equal(f.responses().length,2);assert.equal(f.voice.state,'thinking');f.done('tail');assert.equal(f.voice.state,'listening');f.sameSession();
});

test('native finalized results contain only the customer reply and never private host receipt fields',async t=>{
  const f=fixture(t,{finalized:true});await f.voice.start();f.speech();await settle();assert.equal(f.hostCalls(),1);assert.equal(f.responses().length,1);assert.equal(f.responses()[0].response.tool_choice,'none');
  const providerText=JSON.stringify(f.sent);assert.match(providerText,/The necklace is now in your cart/);assert.doesNotMatch(providerText,/NEVER SPEAK|INTERNAL PRIVATE|completedActions|postcondition/);f.sameSession();
});

for(const failure of [false,true])test('false VAD barge-in with '+(failure?'failed':'empty')+' ASR resumes only the sanitized completed reply',async t=>{
  const f=fixture(t,{finalized:true});await f.voice.start();f.speech();await settle();f.bind();f.output('response-current');f.done();
  f.speech('empty-input',failure?null:'');if(failure)f.emit({type:'conversation.item.input_audio_transcription.failed',item_id:'empty-input'});await settle();assert.equal(f.hostCalls(),1);assert.equal(f.responses().length,2);assert.equal(f.responses()[1].response.tool_choice,'none');assert.match(f.responses()[1].response.instructions,/already-completed reply/);assert.doesNotMatch(JSON.stringify(f.sent),/NEVER SPEAK|INTERNAL PRIVATE/);f.sameSession();
});

test('late finalized words after failed-ASR speech recovery cannot revive shopping authority',async t=>{
  const f=fixture(t,{finalized:true});await f.voice.start();f.speech();await settle();f.bind();f.output('response-current');f.done();f.speech('failed-input',null);f.emit({type:'conversation.item.input_audio_transcription.failed',item_id:'failed-input'});await settle();assert.equal(f.hostCalls(),1);assert.equal(f.responses().length,2);
  f.emit({type:'conversation.item.input_audio_transcription.completed',item_id:'failed-input',transcript:'Add three more necklaces to my cart'});await settle();assert.equal(f.hostCalls(),1);assert.equal(f.responses().length,2);assert.equal(f.responses()[1].response.tool_choice,'none');f.sameSession();
});

test('genuine new shopper words supersede the interrupted answer and empty ASR never creates a new action',async t=>{
  const f=fixture(t,{finalized:true});await f.voice.start();f.speech();await settle();f.bind();f.output('response-current');f.done();f.speech('new-input','Take me to the homepage');await settle();assert.equal(f.hostCalls(),2);assert.equal(f.responses().length,2);assert.doesNotMatch(f.responses()[1].response.instructions,/already-completed reply/);f.sameSession();
});

test('empty/failed ASR recovery is bounded to two resumes for the same safe reply',async t=>{
  const f=fixture(t,{finalized:true});await f.voice.start();f.speech();await settle();let active='response-current';f.bind(active);f.output(active);f.done(active);
  for(let n=0;n<3;n++){f.speech('noise-'+n,null);f.emit({type:'conversation.item.input_audio_transcription.failed',item_id:'noise-'+n});await settle();if(n<2){assert.equal(f.responses().length,n+2);active='resumed-'+n;f.bind(active);f.output(active);f.done(active);}}
  assert.equal(f.responses().length,3);assert.equal(f.hostCalls(),1);assert.equal(f.voice.state,'listening');assert.equal(f.warnings.length,1);f.sameSession();
});

test('hard native expiry gives advance notice and confirmed stop before a safe reply-only renewal',async t=>{
  const f=fixture(t,{finalized:true});await f.voice.start();await f.advance(105000);assert.deepEqual(f.events.find(v=>v[0]==='expiring'),['expiring',{remainingMs:15000}]);
  f.speech();await settle();f.bind();f.output('response-current');f.done();await f.advance(15000);assert.equal(f.voice.state,'idle');assert.equal(f.requests.filter(v=>v.action==='start').length,1);assert.equal(f.requests.filter(v=>v.action==='stop').length,1);
  const ended=f.events.find(v=>v[0]==='stopped');assert.equal(ended[1],'limit');assert.equal(ended[2].serverStopped,true);assert.equal(ended[2].unfinishedReply,'The necklace is now in your cart.');assert.ok(f.events.findIndex(v=>v[0]==='closed')<f.events.findIndex(v=>v[0]==='stopped'));
  assert.equal(await f.voice.start({continuationReply:ended[2].unfinishedReply}),true);const response=f.responses().at(-1).response;assert.equal(response.tool_choice,'none');assert.match(response.instructions,/The necklace is now in your cart/);assert.equal(f.hostCalls(),1);assert.equal(f.toolCalls(),0);
});

test('unconfirmed remote stop is explicit at expiry and the adapter never starts a replacement provider call',async t=>{
  const f=fixture(t,{hangStop:true});await f.voice.start();await f.advance(120000);assert.equal(f.voice.state,'closing');assert.equal(f.events.filter(v=>v[0]==='stopped').length,0);await f.advance(5500);assert.equal(f.voice.state,'idle');assert.equal(f.events.find(v=>v[0]==='stopped')[2].serverStopped,false);assert.equal(f.requests.filter(v=>v.action==='start').length,1);
});

test('manual stop removes the old expiry notice and cannot renew on a delayed callback',async t=>{
  const f=fixture(t);await f.voice.start();await f.voice.stop();await f.advance(120000);assert.equal(f.events.filter(v=>v[0]==='expiring'||v[0]==='expired').length,0);assert.equal(f.requests.filter(v=>v.action==='start').length,1);
});

test('opted-in session renewal without an unfinished reply starts listening and never greets or takes action',async t=>{
  const f=fixture(t);await f.voice.start();await f.voice.stop();await f.voice.start({renewal:true});assert.equal(f.voice.state,'listening');assert.equal(f.responses().length,0);assert.equal(f.hostCalls(),0);assert.equal(f.toolCalls(),0);
});

for(const retryAfterMs of [1,23000,60000])test('native429 exposes only its bounded '+retryAfterMs+'ms renewal delay without an automatic new call',async t=>{
  const f=fixture(t);f.runtime.fetch=async()=>Response.json({code:'VOICE_RATE_LIMITED',retryAfterMs,providerDetail:'PRIVATE ACCOUNT METADATA'},{status:429});assert.equal(await f.voice.start({renewal:true}),false);assert.equal(f.voice.lastError.code,'VOICE_RATE_LIMITED');assert.equal(f.voice.lastError.retryAfterMs,retryAfterMs);assert.doesNotMatch(JSON.stringify(f.voice.lastError),/PRIVATE|METADATA/);assert.equal(f.responses().length,0);assert.equal(f.hostCalls(),0);
});

for(const retryAfterMs of [0,-1,60001,1.5,'23000',null])test('invalid '+JSON.stringify(retryAfterMs)+' native429 delay cannot schedule a client renewal',async t=>{
  const f=fixture(t);f.runtime.fetch=async()=>Response.json({code:'VOICE_RATE_LIMITED',retryAfterMs},{status:429});assert.equal(await f.voice.start({renewal:true}),false);assert.equal(f.voice.lastError.code,'VOICE_RATE_LIMITED');assert.equal(Object.hasOwn(f.voice.lastError,'retryAfterMs'),false);assert.equal(f.responses().length,0);
});

test('provider errors or arbitrary thrownError properties cannot forge our endpoint429 retry delay',async t=>{
  const fake=Object.assign(Error(native.MESSAGES.rate),{retryAfterMs:1000});assert.equal(Object.hasOwn(native.publicFailure(fake),'retryAfterMs'),false);
  for(const response of [Response.json({code:'VOICE_RATE_LIMITED',retryAfterMs:1000},{status:503}),Response.json({code:'PRIVATE_PROVIDER_CODE',retryAfterMs:1000},{status:429})]){const f=fixture(t);f.runtime.fetch=async()=>response.clone();assert.equal(await f.voice.start({renewal:true}),false);assert.equal(Object.hasOwn(f.voice.lastError,'retryAfterMs'),false);}
});

test('customer reply projection admits natural product wording and rejects control diagnostics',()=>{
  assert.equal(native.shopperReply({reply:'This necklace is available in 14k solid gold.',completedActions:[{private:'never'}]}),'This necklace is available in 14k solid gold.');
  assert.equal(native.shopperReply({customerMessage:'Your cart is open.',reply:'no further action was completed'}),'Your cart is open.');
  for(const reply of ['Motif and no further action was completed','{"variantId":"secret"}','The backend could not prepare this.'])assert.equal(native.shopperReply({reply}),'I couldn’t finish that request. Please try again.');
});
