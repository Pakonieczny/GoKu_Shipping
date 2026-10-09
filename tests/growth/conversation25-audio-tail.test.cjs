'use strict';
// Actual native adapter, synthetic data channel/media. No OpenAI request,
// microphone, physical audio playback or end-to-end latency claim.
const test=require('node:test'),assert=require('node:assert/strict');
const adapter=require('../../brites-concierge-voice.js');
const SDP='v=0\r\nm=audio 9 UDP/TLS/RTP/SAVPF 111\r\na=candidate:1 1 UDP 2122260223 192.0.2.10 50000 typ host\r\n';
function fixture(t){
  let channel,peer,nextTimer=0;const sent=[],states=[],errors=[],tools=[],timers=new Map();
  const track={stop(){}},stream={getTracks:()=>[track],getAudioTracks:()=>[track]};
  const document={hidden:false,body:{appendChild(){}},createElement:()=>({setAttribute(){},play:async()=>{},pause(){},remove(){},srcObject:null}),addEventListener(){},removeEventListener(){}};
  class Peer{
    constructor(){peer=this;this.iceGatheringState='complete';}
    addTrack(){}createDataChannel(){channel={readyState:'connecting',send:raw=>sent.push(JSON.parse(raw)),close(){this.readyState='closed';}};return channel;}
    async createOffer(){return {type:'offer',sdp:SDP};}async setLocalDescription(value){this.localDescription=value;}
    async setRemoteDescription(){channel.readyState='open';channel.onopen?.();}close(){}
  }
  const runtime={document,location:{origin:'https://preview.test'},navigator:{mediaDevices:{getUserMedia:async()=>stream}},RTCPeerConnection:Peer,AbortController,
    setTimeout(fn,ms){const id=++nextTimer;timers.set(id,{fn,ms});return id;},clearTimeout:id=>timers.delete(id),addEventListener(){},removeEventListener(){},
    fetch:async(url,init)=>{const body=JSON.parse(init.body);return Response.json(body.action==='capabilities'?{enabled:true}:body.action==='start'?{sdp:SDP,stopToken:'synthetic-stop-token',maxDurationMs:120000}:{stopped:true});}};
  const voice=adapter.create({runtime,greeting:false,onState:state=>states.push(state),onError:error=>errors.push(error),onTool:async(args,context)=>{tools.push({args,context});return {verified:true};}});
  t.after(async()=>{await voice.dispose();assert.equal(timers.size,0);});
  const emit=event=>channel?.onmessage?.({data:JSON.stringify(event)});
  const responses=input=>sent.filter(event=>event.type==='response.create'&&(!input||event.response.metadata.brites_input_item===input));
  function bind(id,input='input-current'){
    emit({type:'input_audio_buffer.speech_started',item_id:input});emit({type:'input_audio_buffer.speech_stopped',item_id:input});emit({type:'input_audio_buffer.committed',item_id:input});
    const request=responses(input).at(-1);emit({type:'response.created',response:{id,metadata:request.response.metadata}});return request;
  }
  function bindIssued(id,request){emit({type:'response.created',response:{id,metadata:request.response.metadata}});}
  function done(id,status='completed',reason){emit({type:'response.done',response:{id,status,...(reason?{status_details:{type:'incomplete',reason}}:{})}});}
  return {voice,emit,sent,states,errors,tools,timers,responses,bind,bindIssued,done,peer:()=>peer};
}
test('generation completion leaves the current speaking pose and buffered audio untouched until playback stops',async t=>{
  const f=fixture(t);await f.voice.start();f.bind('response-current');f.emit({type:'output_audio_buffer.started',response_id:'response-current'});
  assert.equal(f.voice.state,'speaking');const clears=f.sent.filter(event=>event.type==='output_audio_buffer.clear').length;
  f.done('response-current');assert.equal(f.voice.state,'speaking');assert.equal(f.sent.filter(event=>event.type==='output_audio_buffer.clear').length,clears);assert.equal(f.responses('input-current').length,1);
  f.emit({type:'output_audio_buffer.stopped',response_id:'response-current'});assert.equal(f.voice.state,'listening');assert.equal(f.responses('input-current').length,1);
});
test('a token-limited response waits for its audio drain and permits at most two tool-free same-turn tails',async t=>{
  const f=fixture(t);await f.voice.start();const initial=f.bind('response-current');f.emit({type:'output_audio_buffer.started',response_id:'response-current'});
  f.done('response-current','incomplete','max_output_tokens');assert.equal(f.voice.state,'speaking');assert.equal(f.responses('input-current').length,1);
  f.emit({type:'output_audio_buffer.stopped',response_id:'response-current'});const requests=f.responses('input-current');assert.equal(requests.length,2);const continuation=requests[1];assert.equal(continuation.response.tool_choice,'none');assert.equal(continuation.response.metadata.brites_input_item,'input-current');assert.equal(continuation.response.metadata.brites_turn_version,initial.response.metadata.brites_turn_version);assert.notEqual(continuation.response.metadata.brites_voice_request,initial.response.metadata.brites_voice_request);
  f.emit({type:'output_audio_buffer.stopped',response_id:'response-current'});f.done('response-current','incomplete','max_output_tokens');assert.equal(f.responses('input-current').length,2);
  assert.equal(continuation.response.max_output_tokens,1200);f.bindIssued('response-tail',continuation);f.emit({type:'output_audio_buffer.started',response_id:'response-tail'});f.done('response-tail','incomplete','max_output_tokens');f.emit({type:'output_audio_buffer.stopped',response_id:'response-tail'});assert.equal(f.responses('input-current').length,3);const finalTail=f.responses('input-current')[2];assert.equal(finalTail.response.tool_choice,'none');assert.equal(finalTail.response.max_output_tokens,1200);assert.equal(finalTail.response.metadata.brites_input_item,'input-current');f.bindIssued('response-final-tail',finalTail);f.emit({type:'output_audio_buffer.started',response_id:'response-final-tail'});f.done('response-final-tail','incomplete','max_output_tokens');f.emit({type:'output_audio_buffer.stopped',response_id:'response-final-tail'});assert.equal(f.responses('input-current').length,3,'a third incomplete reply cannot form an unbounded continuation loop');
});
test('playback that drains before generation completion permits only one bounded continuation once incompleteness is known',async t=>{
  const f=fixture(t);await f.voice.start();f.bind('response-current');f.emit({type:'output_audio_buffer.started',response_id:'response-current'});f.emit({type:'output_audio_buffer.stopped',response_id:'response-current'});assert.equal(f.responses('input-current').length,1);
  f.done('response-current','incomplete','max_output_tokens');assert.equal(f.responses('input-current').length,2);assert.equal(f.responses('input-current')[1].response.tool_choice,'none');
});
for(const reason of ['speech','manual interruption','stop'])test(reason+' cancels a pending audio-tail continuation and rejects stale completion/playback events',async t=>{
  const f=fixture(t);await f.voice.start();f.bind('response-old');f.emit({type:'output_audio_buffer.started',response_id:'response-old'});f.done('response-old','incomplete','max_output_tokens');
  if(reason==='speech')f.emit({type:'input_audio_buffer.speech_started',item_id:'input-new'});else if(reason==='manual interruption')f.voice.interrupt();else await f.voice.stop();
  f.emit({type:'output_audio_buffer.stopped',response_id:'response-old'});f.done('response-old','incomplete','max_output_tokens');assert.equal(f.responses('input-current').length,1);assert.notEqual(f.voice.state,'speaking');
});
for(const reason of ['content_filter','turn_detected','other'])test('incomplete '+reason+' does not automatically repeat or bypass the reason',async t=>{
  const f=fixture(t);await f.voice.start();f.bind('response-current');f.emit({type:'output_audio_buffer.started',response_id:'response-current'});f.done('response-current','incomplete',reason);f.emit({type:'output_audio_buffer.stopped',response_id:'response-current'});assert.equal(f.responses('input-current').length,1);
});
test('a failed or unbound response never initiates continuation',async t=>{
  const f=fixture(t);await f.voice.start();f.bind('response-current');f.done('response-current','failed');assert.equal(f.responses('input-current').length,1);
  f.done('response-unbound','incomplete','max_output_tokens');f.emit({type:'output_audio_buffer.stopped',response_id:'response-unbound'});assert.equal(f.responses('input-current').length,1);
});
test('a continuation response cannot invoke product tools even if a provider event violates tool_choice none',async t=>{
  const f=fixture(t);await f.voice.start();f.bind('response-current');f.emit({type:'output_audio_buffer.started',response_id:'response-current'});f.done('response-current','incomplete','max_output_tokens');f.emit({type:'output_audio_buffer.stopped',response_id:'response-current'});
  const tail=f.responses('input-current')[1];f.bindIssued('response-tail',tail);f.emit({type:'response.function_call_arguments.done',response_id:'response-tail',call_id:'tail-unsafe-call',name:'find_jewellery',arguments:'{"message":"Show necklaces"}'});
  await new Promise(resolve=>setImmediate(resolve));assert.equal(f.tools.length,0,'audio finishing has no host tool authority');const output=f.sent.find(event=>event.item?.call_id==='tail-unsafe-call');assert.equal(output,undefined,'tool-free audio finishing does not run or chain an extra tool turn');assert.equal(f.responses('input-current').length,2);
});
