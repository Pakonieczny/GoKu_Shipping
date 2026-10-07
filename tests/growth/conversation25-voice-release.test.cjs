'use strict';
// Adversarial event identity and release checks against the actual adapter.
// These synthetic fixtures neither request real media nor contact a provider.
const test=require('node:test'),assert=require('node:assert/strict');
const client=require('../../brites-concierge-voice.js');
const SDP='v=0\r\nm=audio 9 UDP/TLS/RTP/SAVPF 111\r\na=candidate:1 1 UDP 2122260223 192.0.2.10 50000 typ host\r\n';
function fixture(t){
  let channel,timer=0;const sent=[],calls=[],errors=[],warnings=[],timers=new Map();
  const liveTrack={kind:'audio',readyState:'live',stop(){}};const stream={getTracks:()=>[liveTrack],getAudioTracks:()=>[liveTrack]};
  const document={hidden:false,body:{appendChild(){}},createElement:()=>({setAttribute(){},play:async()=>{},pause(){},remove(){}}),addEventListener(){},removeEventListener(){}};
  class Peer{constructor(){this.iceGatheringState='complete';}addTrack(){}createDataChannel(){channel={readyState:'connecting',send:raw=>sent.push(JSON.parse(raw)),close(){}};return channel;}async createOffer(){return {sdp:SDP,type:'offer'};}async setLocalDescription(value){this.localDescription=value;}async setRemoteDescription(){channel.readyState='open';channel.onopen();}close(){}}
  const runtime={document,location:{origin:'https://preview.test'},navigator:{mediaDevices:{getUserMedia:async()=>stream}},RTCPeerConnection:Peer,AbortController,setTimeout(fn,ms){const id=++timer;timers.set(id,{fn,ms});return id;},clearTimeout:id=>timers.delete(id),addEventListener(){},removeEventListener(){},fetch:async(url,init)=>Response.json(JSON.parse(init.body).action==='start'?{sdp:SDP,stopToken:'synthetic-stop-token',maxDurationMs:120000}:{enabled:true,stopped:true})};
  const voice=client.create({runtime,greeting:false,onError:error=>errors.push(error),onTurnWarning:warning=>warnings.push(warning),onTool:async(args,context)=>{calls.push({args,context});return {verified:true};}});
  t.after(()=>voice.dispose());
  const emit=event=>channel.onmessage({data:JSON.stringify(event)}),responses=()=>sent.filter(event=>event.type==='response.create');
  function startTurn(){emit({type:'input_audio_buffer.speech_started',item_id:'input-current'});emit({type:'input_audio_buffer.speech_stopped',item_id:'input-current'});emit({type:'input_audio_buffer.committed',item_id:'input-current'});emit({type:'response.created',response:{id:'response-current',metadata:responses().at(-1).response.metadata}});}
  function tail(){startTurn();emit({type:'output_audio_buffer.started',response_id:'response-current'});emit({type:'response.done',response:{id:'response-current',status:'incomplete',status_details:{reason:'max_output_tokens'}}});emit({type:'output_audio_buffer.stopped',response_id:'response-current'});emit({type:'response.created',response:{id:'response-tail',metadata:responses().at(-1).response.metadata}});}
  return {voice,emit,startTurn,tail,responses,sent,calls,errors,warnings};
}
test('malformed function events without response identity cannot escape an audio-only continuation',async t=>{
  const h=fixture(t);await h.voice.start();h.tail();assert.equal(h.responses().length,2);
  h.emit({type:'response.function_call_arguments.done',call_id:'missing-response',name:'find_jewellery',arguments:'{"message":"Bunny necklaces"}'});await new Promise(resolve=>setImmediate(resolve));assert.equal(h.calls.length,0);assert.equal(h.responses().length,2);
});
test('missing response identity cannot authorize a new paid audio continuation by arrival timing',async t=>{
  const h=fixture(t);await h.voice.start();h.startTurn();assert.equal(h.responses().length,1);
  h.emit({type:'response.done',response:{status:'incomplete',status_details:{reason:'max_output_tokens'}}});assert.equal(h.responses().length,1);
});
test('recoverable provider errors and failed replies leave the connected voice alive without exposing raw data',async t=>{
  const h=fixture(t);await h.voice.start();h.startTurn();h.emit({type:'output_audio_buffer.started',response_id:'response-current'});
  h.emit({type:'error',error:{code:'invalid_request_error',message:'PRIVATE_API_KEY https://private.example'}});assert.equal(h.voice.state,'speaking');assert.equal(h.errors.length,0);assert.equal(h.warnings.length,1);assert.doesNotMatch(JSON.stringify(h.warnings),/PRIVATE_API_KEY|private\.example/);
  h.emit({type:'response.done',response:{id:'response-current',status:'failed'}});assert.equal(h.voice.state,'speaking');assert.equal(h.errors.length,0);assert.equal(h.warnings.length,2);
  h.emit({type:'output_audio_buffer.stopped',response_id:'response-current'});assert.equal(h.voice.state,'listening');
});
