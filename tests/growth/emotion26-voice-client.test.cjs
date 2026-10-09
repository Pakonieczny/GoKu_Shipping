'use strict';
// Actual native client with synthetic WebRTC/data-channel/clock only. No
// provider calls, audible-media, physical microphone or GPU certification.
const test=require('node:test'),assert=require('node:assert/strict');
const client=require('../../brites-concierge-voice.js');
const SDP='v=0\r\nm=audio 9 UDP/TLS/RTP/SAVPF 111\r\na=candidate:1 1 UDP 2122260223 192.0.2.10 50000 typ host\r\n';
const cue={mood:'curious',gesture:'present',intensity:.45,durationMs:1200};
const flush=()=>new Promise(resolve=>setImmediate(resolve));
function fixture(t,{greeting=false,callback=true}={}){
  let channel,timer=0,input=0,response=0;const sent=[],calls=[],performances=[],cancelled=[],states=[],timers=new Map(),listeners=new Map();
  const liveTrack={kind:'audio',readyState:'live',stop(){}};const stream={getTracks:()=>[liveTrack],getAudioTracks:()=>[liveTrack]};
  const document={hidden:false,body:{appendChild(){}},createElement:()=>({setAttribute(){},play:async()=>{},pause(){},remove(){}}),addEventListener:(name,fn)=>listeners.set(name,fn),removeEventListener:name=>listeners.delete(name)};
  class Peer{constructor(){this.iceGatheringState='complete';}addTrack(){}createDataChannel(){channel={readyState:'connecting',send:raw=>sent.push(JSON.parse(raw)),close(){}};return channel;}async createOffer(){return {sdp:SDP,type:'offer'};}async setLocalDescription(value){this.localDescription=value;}async setRemoteDescription(){channel.readyState='open';channel.onopen();}close(){}}
  const runtime={document,location:{origin:'https://preview.test'},navigator:{mediaDevices:{getUserMedia:async()=>stream}},RTCPeerConnection:Peer,AbortController,setTimeout(fn,ms){const id=++timer;timers.set(id,{fn,ms});return id;},clearTimeout:id=>timers.delete(id),addEventListener:(name,fn)=>listeners.set(name,fn),removeEventListener:name=>listeners.delete(name),fetch:async(url,init)=>Response.json(JSON.parse(init.body).action==='start'?{sdp:SDP,stopToken:'synthetic-stop-token',maxDurationMs:120000}:{enabled:true,stopped:true})};
  const voice=client.create({runtime,greeting,onState:value=>states.push(value),...(callback?{onAvatarPerformance:(value,context)=>performances.push({value,context})}:{}),onAvatarPerformanceCancelled:value=>cancelled.push(value),onTool:async(args,context)=>{calls.push({args,context});return {verified:true,products:[]};}});
  t.after(async()=>{await voice.dispose();assert.equal(timers.size,0);});
  const emit=event=>channel.onmessage?.({data:JSON.stringify(event)}),responses=()=>sent.filter(event=>event.type==='response.create');
  function bind(id='response-'+(++response),metadata=responses().at(-1)?.response.metadata){emit({type:'response.created',response:{id,metadata}});return id;}
  function begin(){const item='input-'+(++input);emit({type:'input_audio_buffer.speech_started',item_id:item});emit({type:'input_audio_buffer.speech_stopped',item_id:item});emit({type:'input_audio_buffer.committed',item_id:item});return {item,id:bind()};}
  function expression(id,args=cue,callId='cue-'+sent.length){emit({type:'response.function_call_arguments.done',response_id:id,call_id:callId,name:'set_avatar_performance',arguments:JSON.stringify(args)});return callId;}
  function tool(id,name='find_jewellery',callId='shop-'+sent.length){emit({type:'response.function_call_arguments.done',response_id:id,call_id:callId,name,arguments:JSON.stringify(name==='prepare_jewellery_action'?{handle:'bunny-necklace',action:'options'}:{message:'A bunny necklace'})});return callId;}
  const output=id=>{const item=sent.find(event=>event.item?.call_id===id)?.item;return item?JSON.parse(item.output):null;};
  return {voice,document,sent,calls,performances,cancelled,states,emit,responses,bind,begin,expression,tool,output,listeners};
}

test('presentation validator accepts only the exact closed enum and numeric envelope',()=>{
  for(const mood of ['calm','curious','warm','celebrate','reassuring','appreciated'])for(const gesture of ['none','greet','acknowledge','focus','explain','present','reassure','confirm'])assert.deepEqual(client.validateToolArguments({...cue,mood,gesture},'set_avatar_performance'),{...cue,mood,gesture});
  for(const value of [{...cue,intensity:0,durationMs:400},{...cue,intensity:1,durationMs:2500}])assert.deepEqual(client.validateToolArguments(value,'set_avatar_performance'),value);
});
test('presentation validation rejects coercion, unknown/private/action fields and out-of-range values',()=>{
  const values=[null,[],{}, {...cue,mood:'angry'}, {...cue,gesture:'cart'}, {...cue,handle:'bunny'}, {...cue,url:'https://evil.example'}, {...cue,variantId:'101'}, {...cue,accountId:'secret'}, {...cue,intensity:'0.4'}, {...cue,intensity:NaN}, {...cue,intensity:Infinity}, {...cue,intensity:-.01}, {...cue,intensity:1.01}, {...cue,durationMs:'1000'}, {...cue,durationMs:399}, {...cue,durationMs:2501}, {...cue,durationMs:1000.1}];
  for(const value of values)assert.equal(client.validateToolArguments(value,'set_avatar_performance'),null);for(const key of Object.keys(cue)){const value={...cue};delete value[key];assert.equal(client.validateToolArguments(value,'set_avatar_performance'),null);}
});
test('current bound expression emits a safe callback and ack without host tools, new speech or audio-state mutation',async t=>{
  const h=fixture(t);await h.voice.start();const turn=h.begin(),before=h.responses().length,state=h.voice.state;const call=h.expression(turn.id);assert.equal(h.performances.length,1);assert.deepEqual(h.performances[0].value,cue);assert.deepEqual(h.performances[0].context,{responseId:turn.id,inputItemId:turn.item,turnVersion:2,currentTurn:true});assert.deepEqual(h.output(call),{accepted:true,presentationOnly:true});assert.equal(h.calls.length,0);assert.equal(h.responses().length,before);assert.equal(h.voice.state,state);
});
test('duplicate and later expression calls are limited to one accepted cue per shopper turn',async t=>{
  const h=fixture(t);await h.voice.start();const turn=h.begin(),id=h.expression(turn.id,cue,'cue-1');h.expression(turn.id,cue,id);const second=h.expression(turn.id,{...cue,mood:'warm'},'cue-2');assert.equal(h.performances.length,1);assert.equal(h.sent.filter(event=>event.item?.call_id===id).length,1);assert.deepEqual(h.output(second),{accepted:false,presentationOnly:true});const next=h.begin();h.expression(next.id,{...cue,mood:'warm'});assert.equal(h.performances.length,2);
});
test('invalid expression input acknowledges refusal without spending the permitted valid cue',async t=>{
  const h=fixture(t);await h.voice.start();const turn=h.begin(),bad=h.expression(turn.id,{...cue,action:'navigate'});assert.deepEqual(h.output(bad),{accepted:false,presentationOnly:true});h.expression(turn.id);assert.equal(h.performances.length,1);assert.equal(h.calls.length,0);assert.equal(h.responses().length,1);
});
test('a missing host presentation callback fails safely without an extra response',async t=>{
  const h=fixture(t,{callback:false});await h.voice.start();const turn=h.begin(),id=h.expression(turn.id);assert.deepEqual(h.output(id),{accepted:false,presentationOnly:true});h.emit({type:'response.done',response:{id:turn.id,status:'completed',output:[]}});assert.equal(h.responses().length,1);
});
for(const invalid of ['missing','unknown','forged','uncommitted'])test('expression requires exact issued active response identity: '+invalid,async t=>{
  const h=fixture(t);await h.voice.start();let id;if(invalid==='uncommitted'){h.emit({type:'input_audio_buffer.speech_started',item_id:'uncommitted'});h.emit({type:'input_audio_buffer.speech_stopped',item_id:'uncommitted'});id=h.bind('uncommitted-response',{});}else {h.begin();id=invalid==='missing'?undefined:invalid==='unknown'?'unknown':h.bind('forged',{brites_voice_request:'not-issued',brites_input_item:'input-1',brites_turn_version:'2'});}const before=h.sent.length;h.expression(id);assert.equal(h.performances.length,0);assert.equal(h.calls.length,0);assert.equal(h.sent.length,before);
});
for(const boundary of ['interrupt','new speech','response done','hidden','stop'])test(boundary+' rejects late expression and cancels visual authority when appropriate',async t=>{
  const h=fixture(t);await h.voice.start();const turn=h.begin();if(boundary==='interrupt')h.voice.interrupt();else if(boundary==='new speech')h.emit({type:'input_audio_buffer.speech_started',item_id:'new-input'});else if(boundary==='response done')h.emit({type:'response.done',response:{id:turn.id,status:'completed',output:[]}});else if(boundary==='hidden')h.document.hidden=true;else await h.voice.stop();const before=h.responses().length;h.expression(turn.id);assert.equal(h.performances.length,0);assert.equal(h.calls.length,0);assert.equal(h.responses().length,before);if(['interrupt','new speech','stop'].includes(boundary))assert.ok(h.cancelled.length>=2);
});
test('an older response in the same shopper turn cannot act after a newer response becomes active',async t=>{
  const h=fixture(t);await h.voice.start();const turn=h.begin();h.tool(turn.id);await flush();h.emit({type:'response.done',response:{id:turn.id,status:'completed'}});const newer=h.bind('response-next'),before=h.responses().length;h.expression(turn.id);assert.equal(h.performances.length,0);h.expression(newer);assert.equal(h.performances.length,1);assert.equal(h.responses().length,before);
});
test('expressions do not consume the three-call shopping budget or close its checked-read chain',async t=>{
  const h=fixture(t);await h.voice.start();const turn=h.begin();h.expression(turn.id);let active=turn.id;for(let i=0;i<3;i++){h.tool(active,'find_jewellery','shop-'+i);await flush();h.emit({type:'response.done',response:{id:active,status:'completed'}});active=h.bind('after-shop-'+i);}assert.equal(h.calls.length,3);assert.equal(h.responses().length,4);assert.equal(h.responses().at(-1).response.tool_choice,'none');assert.ok(h.calls.every(call=>call.context.name==='find_jewellery'));
});
test('disabled greeting response cannot request expression or tool-only speech',async t=>{
  const h=fixture(t,{greeting:true});await h.voice.start();const id=h.bind('greeting-response'),before=h.sent.length;h.expression(id);h.emit({type:'response.done',response:{id,status:'completed',output:[]}});assert.equal(h.performances.length,0);assert.equal(h.calls.length,0);assert.equal(h.responses().length,1);assert.equal(h.sent.length,before);
});
test('tool-only completed response receives at most one deferred bounded tool-free spoken answer',async t=>{
  const h=fixture(t);await h.voice.start();const turn=h.begin();h.expression(turn.id);assert.equal(h.responses().length,1);h.emit({type:'response.done',response:{id:turn.id,status:'completed',output:[]}});assert.equal(h.responses().length,2);const follow=h.responses().at(-1).response;assert.equal(follow.tool_choice,'none');assert.equal(follow.max_output_tokens,400);assert.match(follow.instructions,/Do not call tools/);const tail=h.bind('presentation-audio-answer');const before=h.sent.length;h.expression(tail);h.emit({type:'response.done',response:{id:tail,status:'completed',output:[]}});h.emit({type:'response.done',response:{id:turn.id,status:'completed',output:[]}});assert.equal(h.performances.length,1);assert.equal(h.responses().length,2);assert.equal(h.sent.length,before);
});
for(const content of ['playing audio','spoken transcript','response output audio','response output text'])test('expression alongside '+content+' creates no additional speech',async t=>{
  const h=fixture(t);await h.voice.start();const turn=h.begin();h.expression(turn.id);let output=[];if(content==='playing audio')h.emit({type:'output_audio_buffer.started',response_id:turn.id});else if(content==='spoken transcript')h.emit({type:'response.output_audio_transcript.delta',response_id:turn.id,delta:'You’re doing fine.'});else output=[{type:'message',content:[{type:content.endsWith('audio')?'audio':'output_text'}]}];h.emit({type:'response.done',response:{id:turn.id,status:'completed',output}});assert.equal(h.responses().length,1);assert.equal(h.performances.length,1);if(content==='playing audio')assert.equal(h.voice.state,'speaking');
});
test('expression plus a shopping tool does not add a second follow-up when the original response completes',async t=>{
  const h=fixture(t);await h.voice.start();const turn=h.begin();h.expression(turn.id);h.tool(turn.id);await flush();assert.equal(h.responses().length,1);h.emit({type:'response.done',response:{id:turn.id,status:'completed',output:[]}});assert.equal(h.responses().length,2);h.emit({type:'response.done',response:{id:turn.id,status:'completed',output:[]}});assert.equal(h.responses().length,2);assert.equal(h.calls.length,1);
});
test('another function in completed response output prevents treating it as expression-only',async t=>{
  const h=fixture(t);await h.voice.start();const turn=h.begin();h.expression(turn.id);h.emit({type:'response.done',response:{id:turn.id,status:'completed',output:[{type:'function_call',name:'find_jewellery',call_id:'not-yet-received'}]}});assert.equal(h.responses().length,1);assert.equal(h.calls.length,0);
});
test('failed expression-only response never schedules another speech response',async t=>{
  const h=fixture(t);await h.voice.start();const turn=h.begin();h.expression(turn.id);h.emit({type:'response.done',response:{id:turn.id,status:'failed',output:[]}});assert.equal(h.responses().length,1);
});
test('max-token audio-tail continuation disables expression, including events with no response identity',async t=>{
  const h=fixture(t);await h.voice.start();const turn=h.begin();h.emit({type:'output_audio_buffer.started',response_id:turn.id});h.emit({type:'response.done',response:{id:turn.id,status:'incomplete',status_details:{reason:'max_output_tokens'}}});h.emit({type:'output_audio_buffer.stopped',response_id:turn.id});const tail=h.bind('audio-tail'),before=h.sent.length;h.expression(tail);h.expression(undefined);assert.equal(h.performances.length,0);assert.equal(h.responses().length,2);assert.equal(h.sent.length,before);
});
test('host progress context passes only approved UI states and never changes authority or creates speech',async t=>{
  for(const progress of ['none','selection-shown','options-shown','review-ready','cart-confirmed','needs-help'])assert.equal(client.publicContext({progress}).progress,progress);for(const progress of ['paid','checkout','order-created',1,{},null])assert.equal(Object.hasOwn(client.publicContext({progress}),'progress'),false);
  const h=fixture(t);await h.voice.start();assert.equal(h.voice.updateContext({pageKind:'product',currentHandle:'bunny',progress:'cart-confirmed'}),true);assert.equal(h.responses().length,0);assert.match(h.sent.at(-1).item.content[0].text,/not a shopper utterance/);assert.equal(h.performances.length,0);assert.equal(h.calls.length,0);
});
