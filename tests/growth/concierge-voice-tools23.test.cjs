'use strict';
// Synthetic data-channel and policy boundary tests. These fixtures do not prove
// physical microphone operation, native audio quality, network ICE, or WebGL.
const test=require('node:test');
const assert=require('node:assert/strict');
const client=require('../../brites-concierge-voice');
const server=require('../../netlify/functions/_britesConciergeVoice');
const SDP='v=0\r\nm=audio 9 UDP/TLS/RTP/SAVPF 111\r\na=candidate:1 1 UDP 2122260223 192.0.2.10 50000 typ host\r\n';
const HANDLE='simple-compass-necklace',VARIANT='gid://shopify/ProductVariant/101';
const flush=()=>new Promise(resolve=>setImmediate(resolve));
function deferred(){let resolve;const promise=new Promise(done=>{resolve=done;});return {promise,resolve};}
function nativeFixture(t,{tool=async()=>({verified:true}),onSpeechStarted}={}){
  let channel,peer,at=0,nextTimer=0,microphones=0,stops=0;
  const calls=[],sent=[],transcripts=[],speech=[],order=[],requests=[],timers=new Map(),listeners=new Map();
  const track={stop(){stops++;}},stream={getTracks:()=>[track],getAudioTracks:()=>[track]};
  const document={hidden:false,body:{appendChild(){}},createElement(){return {setAttribute(){},play:async()=>{},pause(){},remove(){},srcObject:null};},addEventListener(name,fn){listeners.set(name,fn);},removeEventListener(name){listeners.delete(name);}};
  class Peer{
    constructor(){peer=this;this.connectionState='new';this.iceGatheringState='complete';}
    addTrack(){}
    createDataChannel(){channel={readyState:'connecting',send(raw){const value=JSON.parse(raw);sent.push(value);order.push(value.type);},close(){this.readyState='closed';}};return channel;}
    createOffer(){return Promise.resolve({type:'offer',sdp:SDP});}
    setLocalDescription(value){this.localDescription=value;return Promise.resolve();}
    setRemoteDescription(value){this.remoteDescription=value;channel.readyState='open';channel.onopen?.();return Promise.resolve();}
    close(){this.closed=true;}
  }
  const runtime={document,navigator:{mediaDevices:{getUserMedia:async()=>{microphones++;return stream;}}},location:{origin:'https://preview.test'},RTCPeerConnection:Peer,AbortController,
    setTimeout(fn,ms){const id=++nextTimer;timers.set(id,{fn,at:at+ms});return id;},clearTimeout(id){timers.delete(id);},addEventListener(name,fn){listeners.set(name,fn);},removeEventListener(name){listeners.delete(name);},
    fetch:async(url,init)=>{const body=JSON.parse(init.body);requests.push(body);if(body.action==='capabilities')return Response.json({enabled:true});if(body.action==='start')return Response.json({sdp:SDP,stopToken:'fixture-stop-token',maxDurationMs:120000});return Response.json({stopped:true});}};
  const voice=client.create({runtime,greeting:false,onTranscript:value=>transcripts.push(value),onSpeechStarted:value=>{speech.push(value);order.push('authority-invalidated');onSpeechStarted?.(value);},onTool:(args,context)=>{calls.push({args,context});return tool(args,context);}});
  t.after(async()=>{await voice.dispose();assert.equal(timers.size,0);});
  const emit=event=>channel?.onmessage?.({data:JSON.stringify(event)});
  const issued=itemId=>sent.filter(value=>value.type==='response.create'&&value.response.metadata?.brites_input_item===itemId).at(-1)?.response.metadata;
  function begin(itemId,{commit=true,text='Show me the options for the compass necklace',responseId}={}){
    emit({type:'input_audio_buffer.speech_started',item_id:itemId});
    emit({type:'input_audio_buffer.speech_stopped',item_id:itemId});
    if(commit)emit({type:'input_audio_buffer.committed',item_id:itemId});
    if(text!==null)emit({type:'conversation.item.input_audio_transcription.completed',item_id:itemId,transcript:text});
    if(responseId)emit({type:'response.created',response:{id:responseId,...(commit?{metadata:issued(itemId)}:{})}});
  }
  function invoke(name,args,{callId='call-1',responseId,raw}={}){emit({type:'response.function_call_arguments.done',name,call_id:callId,...(responseId?{response_id:responseId}:{}),arguments:raw===undefined?JSON.stringify(args):raw});}
  const output=callId=>{const value=sent.find(event=>event.item?.call_id===callId);return value?JSON.parse(value.item.output):null;};
  return {voice,runtime,calls,sent,transcripts,speech,order,requests,timers,listeners,emit,issued,begin,invoke,output,peer:()=>peer,counts:()=>({microphones,stops}),advance(ms){at+=ms;for(const [id,timer]of [...timers])if(timer.at<=at){timers.delete(id);timer.fn();}}};
}

test('named client and server validators keep search compatibility and exact product inputs',()=>{
  for(const implementation of [client,server]){
    assert.deepEqual(implementation.validateToolArguments({message:' A gift '}),{message:'A gift'});
    assert.deepEqual(implementation.validateToolArguments({message:' A gift '},'find_jewellery'),{message:'A gift'});
    assert.deepEqual(implementation.validateToolArguments({handle:HANDLE},'inspect_jewellery'),{handle:HANDLE});
    for(const action of ['view','options','review'])assert.deepEqual(implementation.validateToolArguments({handle:HANDLE,action},'prepare_jewellery_action'),{handle:HANDLE,action});
    assert.deepEqual(implementation.validateToolArguments({handle:HANDLE,action:'review',variantId:VARIANT},'prepare_jewellery_action'),{handle:HANDLE,action:'review',variantId:VARIANT});
  }
});

test('malformed handles, URLs, unknown fields and coerced inputs fail closed',()=>{
  const handles=['',' '+HANDLE,HANDLE+' ','Upper-Case','../product','a/b','https://shop.test/products/x','a?variant=1','a#x','%61','a\u0000b','-a','a-','a--b','a'.repeat(256),123,new String(HANDLE),null];
  for(const implementation of [client,server]){
    for(const handle of handles){assert.equal(implementation.validateToolArguments({handle},'inspect_jewellery'),null);assert.equal(implementation.validateToolArguments({handle,action:'view'},'prepare_jewellery_action'),null);}
    for(const value of [null,[],{handle:HANDLE,message:'override'},{handle:HANDLE,url:'https://evil.test'},{handle:HANDLE,variantId:VARIANT}])assert.equal(implementation.validateToolArguments(value,'inspect_jewellery'),null);
    assert.equal(implementation.validateToolArguments({handle:HANDLE},'navigate'),null);
    assert.equal(implementation.validateToolArguments({message:new String('gift')}),null);
    assert.equal(implementation.validateToolArguments({message:'gift',action:'view'}),null);
  }
});

test('only an exact ProductVariant GID on review is accepted, without guessing or coercion',()=>{
  const invalid=['101',101,'gid://shopify/Product/101','gid://shopify/ProductVariant/0','gid://shopify/ProductVariant/0101','gid://shopify/ProductVariant/-1','gid://shopify/ProductVariant/1.5',VARIANT+' ',VARIANT+'?x=1','GID://shopify/ProductVariant/101','gid://shopify/ProductVariant/'+('1'.repeat(21)),null,false,new String(VARIANT)];
  for(const implementation of [client,server]){
    for(const variantId of invalid)assert.equal(implementation.validateToolArguments({handle:HANDLE,action:'review',variantId},'prepare_jewellery_action'),null);
    for(const action of ['view','options'])assert.equal(implementation.validateToolArguments({handle:HANDLE,action,variantId:VARIANT},'prepare_jewellery_action'),null);
    for(const action of ['cart','checkout','VIEW',1,new String('view')])assert.equal(implementation.validateToolArguments({handle:HANDLE,action},'prepare_jewellery_action'),null);
    assert.equal(implementation.validateToolArguments({handle:HANDLE,action:'review',quantity:1},'prepare_jewellery_action'),null);
  }
});

test('native session exposes three bounded shop tools and one presentation-only tool',()=>{
  const config=server.sessionConfig();
  assert.deepEqual(config.tools.map(value=>value.name),['find_jewellery','inspect_jewellery','prepare_jewellery_action','set_avatar_performance']);
  for(const tool of config.tools){assert.equal(tool.parameters.additionalProperties,false);assert.equal(tool.type,'function');}
  assert.deepEqual(config.tools[1].parameters.required,['handle']);
  assert.deepEqual(config.tools[2].parameters.required,['handle','action']);
  assert.deepEqual(config.tools[2].parameters.properties.action.enum,['view','options','review']);
  assert.equal(config.max_output_tokens,1200);assert.equal(config.audio.output.voice,'marin');
  assert.equal(config.audio.input.turn_detection.create_response,false);assert.equal(config.audio.input.turn_detection.interrupt_response,true);
  assert.match(config.instructions,/call inspect_jewellery.*before claims about its options, materials/);
  assert.match(config.instructions,/only when the shopper specifically asks/);
  assert.match(config.instructions,/Say a page is being opened only when navigationRequested is true/);
  assert.match(config.instructions,/host verifies the actual current shopper request/);
});

test('constructing native tools does not request media or call a provider',async t=>{
  const f=nativeFixture(t);assert.equal(f.requests.length,0);assert.equal(f.counts().microphones,0);assert.equal(f.voice.state,'idle');
  assert.equal(await f.voice.start(),true);assert.deepEqual(f.requests.map(value=>value.action),['capabilities','start']);
  assert.equal(f.sent.some(value=>value.type==='response.create'),false,'a disabled greeting does not force a catalogue lookup');
});

test('search, inspect and preparation dispatch named contexts for one exact committed speech turn',async t=>{
  const f=nativeFixture(t,{tool:async(args,context)=>context.name==='prepare_jewellery_action'?{verified:true,prepared:true,confirmationRequired:true}:{verified:true,product:{handle:HANDLE}}});
  await f.voice.start();f.begin('input-current',{responseId:'response-current'});
  const turn=f.speech.at(-1).turnVersion;
  f.invoke('find_jewellery',{message:' A compass gift '},{callId:'search',responseId:'response-current'});await flush();
  f.invoke('inspect_jewellery',{handle:HANDLE},{callId:'inspect',responseId:'response-current'});await flush();
  f.invoke('prepare_jewellery_action',{handle:HANDLE,action:'review',variantId:VARIANT},{callId:'prepare',responseId:'response-current'});await flush();
  assert.deepEqual(f.calls.map(value=>value.context.name),['find_jewellery','inspect_jewellery','prepare_jewellery_action']);
  assert.equal(f.calls[0].args.message,'A compass gift');
  for(const call of f.calls){assert.equal(call.context.inputItemId,'input-current');assert.equal(call.context.responseId,'response-current');assert.equal(call.context.turnVersion,turn);assert.equal(call.context.currentTurn,true);assert.equal(call.context.signal.aborted,true,'host signal is retired after completion');}
  assert.deepEqual(f.transcripts.at(-1),{role:'user',text:'Show me the options for the compass necklace',final:true,itemId:'input-current',turnVersion:turn,currentTurn:true});
  assert.equal(f.output('prepare').prepared,true);assert.equal(f.output('prepare').confirmationRequired,true);
});

test('invalid named arguments and missing or unknown tool names never reach host controls',async t=>{
  const f=nativeFixture(t);await f.voice.start();f.begin('input-current',{responseId:'response-current'});
  const events=[['inspect_jewellery',{handle:HANDLE,url:'https://evil.test'}],['prepare_jewellery_action',{handle:HANDLE,action:'view',variantId:VARIANT}],['prepare_jewellery_action',{handle:HANDLE,action:'cart'}],['find_jewellery',{message:'gift',handle:HANDLE}],['navigate',{message:'view'}],[undefined,{message:'gift'}]];
  events.forEach(([name,args],i)=>f.invoke(name,args,{callId:'invalid-'+i,responseId:'response-current'}));
  f.invoke('inspect_jewellery',null,{callId:'invalid-json',responseId:'response-current',raw:'{"handle":'});await flush();
  assert.equal(f.calls.length,0);for(const event of f.sent.filter(value=>value.item)){const output=JSON.parse(event.item.output);assert.equal(output.verified,false);assert.notEqual(output.prepared,true);}
});

test('preparation without a known current response binding cannot borrow current transcript authority',async t=>{
  const f=nativeFixture(t);await f.voice.start();f.begin('input-current');
  f.invoke('prepare_jewellery_action',{handle:HANDLE,action:'view'},{callId:'missing-response'});
  f.invoke('prepare_jewellery_action',{handle:HANDLE,action:'view'},{callId:'unknown-response',responseId:'not-known'});await flush();
  assert.equal(f.calls.length,0);for(const id of ['missing-response','unknown-response']){assert.equal(f.output(id).prepared,false);assert.equal(f.output(id).verified,false);}
});

test('a response created before its exact VAD input commit stays permanently unbound',async t=>{
  const f=nativeFixture(t);await f.voice.start();f.begin('input-current',{commit:false,responseId:'response-unbound'});
  f.emit({type:'input_audio_buffer.committed',item_id:'input-current'});
  f.emit({type:'response.created',response:{id:'response-unbound',metadata:f.issued('input-current')}});
  f.invoke('prepare_jewellery_action',{handle:HANDLE,action:'options'},{responseId:'response-unbound'});await flush();
  assert.equal(f.calls.length,0);assert.equal(f.output('call-1').prepared,false);
});

test('a mismatched or missing input commit cannot authorize current product preparation',async t=>{
  const f=nativeFixture(t);await f.voice.start();f.begin('input-current',{commit:false});
  f.emit({type:'input_audio_buffer.committed',item_id:'other-input'});f.emit({type:'input_audio_buffer.committed'});
  f.emit({type:'response.created',response:{id:'response-uncommitted'}});
  f.invoke('prepare_jewellery_action',{handle:HANDLE,action:'options'},{responseId:'response-uncommitted'});await flush();
  assert.equal(f.calls.length,0);assert.equal(f.output('call-1').prepared,false);assert.equal(f.transcripts.at(-1).currentTurn,false);
});

test('speech authority is invalidated before abort and audio interruption; ignored abort cannot preserve preparation',async t=>{
  const pending=deferred();let signal;
  const f=nativeFixture(t,{tool:(args,context)=>{signal=context.signal;signal.addEventListener('abort',()=>f.order.push('host-aborted'));return pending.promise;}});
  await f.voice.start();f.begin('input-a',{responseId:'response-a'});
  f.invoke('prepare_jewellery_action',{handle:HANDLE,action:'review',variantId:VARIANT},{callId:'preparation-a',responseId:'response-a'});
  assert.equal(signal.aborted,false);const responseCount=f.sent.filter(value=>value.type==='response.create').length;f.order.length=0;f.emit({type:'input_audio_buffer.speech_started',item_id:'input-b'});
  assert.deepEqual(f.order.slice(0,3),['authority-invalidated','host-aborted','response.cancel']);assert.equal(signal.aborted,true);
  await flush();const cancelled=f.output('preparation-a');assert.equal(cancelled.prepared,false);assert.equal(cancelled.cancelled,true);assert.equal(cancelled.verified,false);
  const before=f.sent.length;pending.resolve({verified:true,prepared:true,secret:'stale-result'});await flush();assert.equal(f.sent.length,before);assert.doesNotMatch(JSON.stringify(f.sent),/stale-result/);assert.equal(f.sent.filter(value=>value.type==='response.create').length,responseCount);
});

test('manual interruption expires current authority and discards a stale checked inspection',async t=>{
  const pending=deferred();let signal;const f=nativeFixture(t,{tool:(args,context)=>{signal=context.signal;return pending.promise;}});
  await f.voice.start();f.begin('input-a',{responseId:'response-a'});f.invoke('inspect_jewellery',{handle:HANDLE},{callId:'inspect-a',responseId:'response-a'});
  f.voice.interrupt();assert.equal(f.speech.at(-1).reason,'interrupt');assert.equal(f.speech.at(-1).itemId,'');assert.equal(signal.aborted,true);await flush();
  assert.equal(f.output('inspect-a').verified,false);assert.equal(f.output('inspect-a').cancelled,true);pending.resolve({verified:true,product:{handle:HANDLE,price:123}});await flush();assert.doesNotMatch(JSON.stringify(f.sent),/"price":123/);
});

test('tool deadline aborts a non-responsive preparation and produces only a fixed unprepared result',async t=>{
  let signal;const f=nativeFixture(t,{tool:(args,context)=>{signal=context.signal;return new Promise(()=>{});}});
  await f.voice.start();f.begin('input-current',{responseId:'response-current'});f.invoke('prepare_jewellery_action',{handle:HANDLE,action:'options'},{callId:'deadline',responseId:'response-current'});
  f.advance(14000);await flush();assert.equal(signal.aborted,true);assert.equal(f.output('deadline').prepared,false);assert.equal(f.output('deadline').verified,false);assert.match(f.output('deadline').message,/could not be prepared/);
});

test('late prior transcription remains labelled with its prior turn and cannot upgrade authority',async t=>{
  const f=nativeFixture(t);await f.voice.start();f.begin('input-a',{responseId:'response-a',text:null});const previous=f.speech.at(-1).turnVersion;
  f.begin('input-b',{responseId:'response-b',text:'Show options for this piece'});const current=f.speech.at(-1).turnVersion;
  f.emit({type:'conversation.item.input_audio_transcription.completed',item_id:'input-a',transcript:'Review the former choice'});
  assert.equal(f.transcripts.at(-1).itemId,'input-a');assert.equal(f.transcripts.at(-1).turnVersion,previous);assert.equal(f.transcripts.at(-1).currentTurn,false);assert.ok(current>previous);
  f.emit({type:'conversation.item.input_audio_transcription.completed',item_id:'untracked',transcript:'Review any choice'});assert.equal(f.transcripts.at(-1).turnVersion,null);assert.equal(f.transcripts.at(-1).currentTurn,false);
});

test('an old bound response cannot dispatch a product action in a later committed speech turn',async t=>{
  const f=nativeFixture(t);await f.voice.start();f.begin('input-a',{responseId:'response-a'});f.begin('input-b',{responseId:'response-b'});
  f.invoke('prepare_jewellery_action',{handle:HANDLE,action:'view'},{callId:'old-preparation',responseId:'response-a'});await flush();assert.equal(f.calls.length,0);assert.equal(f.output('old-preparation').prepared,false);assert.equal(f.output('old-preparation').cancelled,true);
  f.invoke('prepare_jewellery_action',{handle:HANDLE,action:'options'},{callId:'current-preparation',responseId:'response-b'});await flush();assert.equal(f.calls.length,1);assert.equal(f.calls[0].context.inputItemId,'input-b');
});

test('A stops, B starts, delayed A response arrives: B completion never binds that old response',async t=>{
  const f=nativeFixture(t);await f.voice.start();f.begin('input-a',{text:null});const metadataA=f.issued('input-a');
  f.emit({type:'input_audio_buffer.speech_started',item_id:'input-b'});
  f.emit({type:'response.created',response:{id:'delayed-response-a',metadata:metadataA}});
  f.emit({type:'input_audio_buffer.speech_stopped',item_id:'input-b'});f.emit({type:'input_audio_buffer.committed',item_id:'input-b'});
  f.emit({type:'conversation.item.input_audio_transcription.completed',item_id:'input-b',transcript:'Show this piece'});
  f.emit({type:'response.created',response:{id:'delayed-response-a',metadata:f.issued('input-b')}});
  f.invoke('prepare_jewellery_action',{handle:HANDLE,action:'view'},{callId:'delayed-a-action',responseId:'delayed-response-a'});await flush();assert.equal(f.calls.length,0);assert.equal(f.output('delayed-a-action').prepared,false);
});

test('all three tool types deduplicate exact call IDs before dispatch',async t=>{
  const f=nativeFixture(t);await f.voice.start();f.begin('input-current',{responseId:'response-current'});
  for(const [name,args]of [['find_jewellery',{message:'A compass gift'}],['inspect_jewellery',{handle:HANDLE}],['prepare_jewellery_action',{handle:HANDLE,action:'options'}]]){f.invoke(name,args,{callId:name,responseId:'response-current'});f.invoke(name,args,{callId:name,responseId:'response-current'});await flush();}
  assert.equal(f.calls.length,3);assert.equal(f.sent.filter(value=>value.item).length,3);
});

test('bad or oversized host results never become checked preparation or leak failure details',async t=>{
  let mode=0;const f=nativeFixture(t,{tool:async()=>{mode++;if(mode===1)return {verified:true,prepared:true,text:'x'.repeat(30001)};if(mode===2)throw Error('private-account-secret');return null;}});
  await f.voice.start();f.begin('input-current',{responseId:'response-current'});
  for(let i=1;i<=3;i++){f.invoke('prepare_jewellery_action',{handle:HANDLE,action:'view'},{callId:'bad-result-'+i,responseId:'response-current'});await flush();assert.equal(f.output('bad-result-'+i).prepared,false);assert.equal(f.output('bad-result-'+i).verified,false);}
  assert.doesNotMatch(JSON.stringify(f.sent),/private-account-secret|x{100}/);
});

test('stop invalidates speech authority and suppresses late action results while media closes',async t=>{
  const pending=deferred();const f=nativeFixture(t,{tool:()=>pending.promise});await f.voice.start();f.begin('input-current',{responseId:'response-current'});
  f.invoke('prepare_jewellery_action',{handle:HANDLE,action:'options'},{callId:'late-after-stop',responseId:'response-current'});
  await f.voice.stop();assert.equal(f.speech.at(-1).reason,'stop');assert.ok(f.peer().closed);assert.ok(f.counts().stops>0);const before=f.sent.length;
  pending.resolve({verified:true,prepared:true});await flush();assert.equal(f.sent.length,before);assert.equal(f.output('late-after-stop'),null);assert.equal(f.voice.state,'idle');
});

test('native commit creates exactly one response with explicit per-turn metadata, without waiting for transcription',async t=>{
  const f=nativeFixture(t);await f.voice.start();f.begin('input-current',{text:null});
  const response=f.sent.filter(value=>value.type==='response.create');assert.equal(response.length,1);assert.equal(response[0].response.tool_choice,'auto');assert.equal(response[0].response.metadata.brites_input_item,'input-current');assert.equal(response[0].response.metadata.brites_turn_version,String(f.speech.at(-1).turnVersion));
  f.emit({type:'input_audio_buffer.committed',item_id:'input-current'});assert.equal(f.sent.filter(value=>value.type==='response.create').length,1,'duplicate commit is not a new inference request');assert.equal(f.transcripts.length,0);
});

test('A response first observed after B commit retains A identity through echoed issued metadata',async t=>{
  const f=nativeFixture(t);await f.voice.start();f.begin('input-a',{text:null});const metadataA=f.issued('input-a');
  f.begin('input-b',{responseId:'response-b',text:'Show options for this piece'});
  f.emit({type:'response.created',response:{id:'delayed-response-a',metadata:metadataA}});
  const responses=f.sent.filter(value=>value.type==='response.create').length;
  f.invoke('prepare_jewellery_action',{handle:HANDLE,action:'options'},{callId:'old-first-seen-after-b',responseId:'delayed-response-a'});await flush();
  assert.equal(f.calls.length,0);assert.equal(f.output('old-first-seen-after-b').prepared,false);assert.equal(f.output('old-first-seen-after-b').cancelled,true);assert.equal(f.sent.filter(value=>value.type==='response.create').length,responses);
});

test('missing, changed or reused response metadata never binds product action authority',async t=>{
  const f=nativeFixture(t);await f.voice.start();f.begin('input-current',{responseId:'response-current'});const metadata=f.issued('input-current');
  const responses=[{id:'missing'},{id:'forged',metadata:{...metadata,brites_voice_request:'not-issued'}},{id:'wrong-input',metadata:{...metadata,brites_input_item:'other-input'}},{id:'wrong-version',metadata:{...metadata,brites_turn_version:'999'}},{id:'reused',metadata}];
  for(const response of responses){f.emit({type:'response.created',response});f.invoke('prepare_jewellery_action',{handle:HANDLE,action:'view'},{callId:'unbound-'+response.id,responseId:response.id});}
  await flush();assert.equal(f.calls.length,0);for(const response of responses)assert.equal(f.output('unbound-'+response.id).prepared,false);
});

test('three successful read tools bound chaining; a fourth cannot call the host and speaks with tools disabled',async t=>{
  const f=nativeFixture(t,{tool:async()=>({verified:true,product:{handle:HANDLE}})});await f.voice.start();f.begin('input-current',{responseId:'response-current'});
  for(let i=1;i<=3;i++){f.invoke('inspect_jewellery',{handle:HANDLE},{callId:'bounded-'+i,responseId:'response-current'});await flush();assert.equal(f.sent.filter(value=>value.type==='response.create').at(-1).response.tool_choice,i<3?'auto':'none');}
  f.invoke('inspect_jewellery',{handle:HANDLE},{callId:'bounded-4',responseId:'response-current'});await flush();assert.equal(f.calls.length,3);assert.equal(f.output('bounded-4').verified,false);assert.equal(f.sent.filter(value=>value.type==='response.create').at(-1).response.tool_choice,'none');
  f.begin('input-next',{responseId:'response-next'});f.invoke('inspect_jewellery',{handle:HANDLE},{callId:'new-turn-read',responseId:'response-next'});await flush();assert.equal(f.calls.length,4);assert.equal(f.sent.filter(value=>value.type==='response.create').at(-1).response.tool_choice,'auto');
});

test('every preparation attempt closes chaining even when host says it could not prepare',async t=>{
  const f=nativeFixture(t,{tool:async()=>({error:'Please choose exact options.'})});await f.voice.start();f.begin('input-current',{responseId:'response-current'});f.invoke('prepare_jewellery_action',{handle:HANDLE,action:'review',variantId:VARIANT},{responseId:'response-current'});await flush();
  assert.equal(f.calls.length,1);assert.equal(f.sent.filter(value=>value.type==='response.create').at(-1).response.tool_choice,'none');
});

test('invalid arguments close chaining and no unchecked host result can restore it in that turn',async t=>{
  const f=nativeFixture(t,{tool:async()=>({verified:true})});await f.voice.start();f.begin('input-current',{responseId:'response-current'});
  f.invoke('inspect_jewellery',{handle:'../bad'},{callId:'invalid',responseId:'response-current'});await flush();assert.equal(f.calls.length,0);assert.equal(f.sent.filter(value=>value.type==='response.create').at(-1).response.tool_choice,'none');
  f.invoke('inspect_jewellery',{handle:HANDLE},{callId:'checked-later',responseId:'response-current'});await flush();assert.equal(f.calls.length,1);assert.equal(f.sent.filter(value=>value.type==='response.create').at(-1).response.tool_choice,'none');
});

test('known stale assistant captions, audio state and response completion cannot overwrite a newer turn',async t=>{
  const f=nativeFixture(t);await f.voice.start();f.begin('input-a',{responseId:'response-a'});f.begin('input-b',{responseId:'response-b'});
  const transcripts=f.transcripts.length;assert.equal(f.voice.state,'thinking');
  for(const type of ['response.output_audio_transcript.delta','response.output_audio_transcript.done','response.audio_transcript.delta','response.audio_transcript.done'])f.emit({type,response_id:'response-a',item_id:'assistant-old',delta:'Old reply',transcript:'Old reply'});
  f.emit({type:'output_audio_buffer.started',response_id:'response-a'});f.emit({type:'response.done',response:{id:'response-a',status:'failed'}});
  assert.equal(f.transcripts.length,transcripts);assert.equal(f.voice.state,'thinking');
  f.emit({type:'response.output_audio_transcript.done',response_id:'response-b',item_id:'assistant-current',transcript:'Your current reply'});assert.equal(f.transcripts.at(-1).text,'Your current reply');f.emit({type:'output_audio_buffer.started',response_id:'response-b'});assert.equal(f.voice.state,'speaking');
});

test('unbound assistant response IDs cannot replace captions or state, while legacy idless read-only events remain compatible',async t=>{
  const f=nativeFixture(t);await f.voice.start();f.begin('input-current',{responseId:'response-current'});const transcripts=f.transcripts.length;
  f.emit({type:'response.output_audio_transcript.done',response_id:'not-bound',transcript:'Unknown reply'});f.emit({type:'output_audio_buffer.started',response_id:'not-bound'});assert.equal(f.transcripts.length,transcripts);assert.equal(f.voice.state,'thinking');
  f.emit({type:'response.output_audio_transcript.done',item_id:'legacy-item',transcript:'Legacy read-only caption'});assert.equal(f.transcripts.at(-1).text,'Legacy read-only caption');
  for(const callId of ['',null,1,'call\u0000invalid','x'.repeat(201)])f.invoke('prepare_jewellery_action',{handle:HANDLE,action:'view'},{callId,responseId:'response-current'});await flush();assert.equal(f.calls.length,0);
});

test('multi-byte host outputs are bounded by UTF-8 bytes and cannot restore a failed tool chain',async t=>{
  const f=nativeFixture(t,{tool:async()=>({verified:true,prepared:true,text:'💎'.repeat(8000)})});await f.voice.start();f.begin('input-current',{responseId:'response-current'});f.invoke('prepare_jewellery_action',{handle:HANDLE,action:'view'},{responseId:'response-current'});await flush();
  assert.equal(f.output('call-1').prepared,false);assert.equal(f.output('call-1').verified,false);assert.doesNotMatch(JSON.stringify(f.sent),/💎/);assert.equal(f.sent.filter(value=>value.type==='response.create').at(-1).response.tool_choice,'none');
});

test('the existing total session tool limit still closes media and the signed session after 100 unique calls',async t=>{
  const f=nativeFixture(t);await f.voice.start();
  for(let i=0;i<100;i++)f.invoke('not-a-tool',{message:'invalid'},{callId:'session-limit-'+i});await flush();assert.notEqual(f.voice.state,'idle');assert.notEqual(f.peer().closed,true);
  f.invoke('not-a-tool',{message:'invalid'},{callId:'session-limit-101'});await flush();assert.equal(f.voice.state,'idle');assert.ok(f.peer().closed);assert.ok(f.counts().stops>0);assert.equal(f.requests.at(-1).action,'stop');assert.equal(f.calls.length,0);
});
