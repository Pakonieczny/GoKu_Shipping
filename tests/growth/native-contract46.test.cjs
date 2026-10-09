'use strict';
// The real voice endpoint and client are exercised through synthetic provider,
// ASR and WebRTC events. These checks do not establish audible model behaviour.
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm');
const {fixture,request,AT,KEY}=require('./voice-fixture36.cjs');
const server=require('../../netlify/functions/_britesConciergeVoice.js');
const voiceSource=fs.readFileSync(require.resolve('../../brites-concierge-voice.js'),'utf8');
const SDP='v=0\r\nm=audio 9 UDP/TLS/RTP/SAVPF 111\r\na=candidate:1 1 UDP 2122260223 192.0.2.10 50000 typ host\r\n';
const HANDLE='otter-blossom-necklace';
const settle=async()=>{for(let i=0;i<8;i++)await new Promise(setImmediate);};
const clone=value=>JSON.parse(JSON.stringify(value));

function native(t){
  const endpoint=fixture(),before=endpoint.protectedBytes(),module={exports:{}};
  // Keep client and durable server clocks aligned without changing global time.
  class Clock extends Date{static now(){return AT;}}
  vm.runInNewContext(voiceSource,{module,Date:Clock,URL,TextEncoder});
  const Voice=module.exports,sent=[],transcripts=[],finalized=[],calls=[],timers=new Map();let channel,timerId=0;
  const document={hidden:false,body:{appendChild(){}},createElement(){return {setAttribute(){},play:async()=>{},pause(){},remove(){}};},addEventListener(){},removeEventListener(){}};
  const track={readyState:'live',stop(){this.readyState='ended';},addEventListener(){},removeEventListener(){}};
  class Peer{
    constructor(){this.iceGatheringState='complete';}addTrack(){}close(){}
    createDataChannel(){channel={readyState:'connecting',send:raw=>sent.push(JSON.parse(raw)),close(){this.readyState='closed';}};return channel;}
    async createOffer(){return {type:'offer',sdp:SDP};}async setLocalDescription(value){this.localDescription=value;}
    async setRemoteDescription(){channel.readyState='open';channel.onopen();}
  }
  const runtime={document,location:{origin:'https://preview.test'},navigator:{mediaDevices:{getUserMedia:async()=>({getTracks:()=>[track],getAudioTracks:()=>[track]})}},RTCPeerConnection:Peer,AbortController,
    setTimeout(fn,ms){const id=++timerId;timers.set(id,{fn,ms});return id;},clearTimeout(id){timers.delete(id);},addEventListener(){},removeEventListener(){},
    fetch:async(url,init)=>endpoint.handler(request(JSON.parse(init.body)))};
  const voice=Voice.create({runtime,greeting:false,onTranscript:value=>transcripts.push(clone(value)),
    onFinalizedTurn:async value=>{finalized.push(value);return {handled:false};},
    onTool:async(args,context)=>{calls.push({args:clone(args),context});return {verified:true,live:true,checkedAt:AT,reply:'The requested published detail is checked.'};}});
  const emit=event=>channel.onmessage({data:JSON.stringify(event)});
  const responses=()=>sent.filter(value=>value.type==='response.create');
  function begin(itemId){emit({type:'input_audio_buffer.speech_started',item_id:itemId});emit({type:'input_audio_buffer.speech_stopped',item_id:itemId});}
  function inspect(itemId,args={handle:HANDLE}){
    const issued=responses().find(value=>value.response.metadata?.brites_input_item===itemId);assert.ok(issued,'inspection must follow a response minted for finalized ASR');
    const responseId='response-'+itemId,callId='call-'+itemId;
    emit({type:'response.created',response:{id:responseId,metadata:issued.response.metadata}});
    emit({type:'response.function_call_arguments.done',name:'inspect_jewellery',response_id:responseId,call_id:callId,arguments:JSON.stringify(args)});return callId;
  }
  const output=callId=>JSON.parse(sent.find(value=>value.item?.call_id===callId).item.output);
  t.after(async()=>{await voice.dispose();assert.equal(timers.size,0);assert.equal(endpoint.protectedBytes(),before,'question handling must preserve usage, budget and controller history');});
  return {voice,Voice,endpoint,emit,begin,inspect,output,responses,transcripts,finalized,calls};
}

test('the actual native provider session receives concise component and length answer rules without relaxing guards',async()=>{
  const f=fixture(),before=f.protectedBytes(),answer=await f.start();assert.equal(answer.response.status,200);
  const config=JSON.parse(f.provider.find(value=>value.url.endsWith('/calls')).init.body.get('session'));
  assert.match(config.instructions,/Answer the specific jewellery question in one short helpful sentence/);
  assert.match(config.instructions,/same shopper input supplies reply or customerMessage/);
  assert.match(config.instructions,/published dimensions of that component only; necklace chain length is not charm size/);
  assert.match(config.instructions,/never infer a diameter from height, width or an unlabeled pair/);
  assert.match(config.instructions,/requested component or dimension is not published, say that briefly/);
  assert.match(config.instructions,/Do not read the full description, headings, packaging, order details/);
  assert.match(config.instructions,/only the distinct published lengths, each once, with their units/);
  assert.match(config.instructions,/Do not recite every variant or combine lengths with metal, engraving, price or stock unless those facts were also requested/);
  assert.match(config.instructions,/What lengths are available is an information question; choose the 16-inch chain is a control request/);
  assert.match(config.instructions,/Never read implementation details, backend terms/);
  assert.match(config.instructions,/Never buy, submit orders, enter payment or contact details/);
  assert.match(config.instructions,/Never echo engraving or gift-note text into summaries or context/);
  assert.equal(config.tools.length,6);assert.equal(config.max_output_tokens,1200);assert.equal(config.audio.output.voice,'marin');
  assert.equal(config.audio.input.turn_detection.create_response,false);assert.equal(config.audio.input.turn_detection.interrupt_response,true);
  assert.equal(answer.value.maxDurationMs,120000);assert.deepEqual(f.deadlines,[{callId:f.journal()[0].callId,expiresAt:AT+120000}]);
  assert.equal(f.protectedBytes(),before);assert.doesNotMatch(JSON.stringify(answer.value),new RegExp(KEY));
  const inspect=config.tools.find(value=>value.name==='inspect_jewellery');
  assert.match(inspect.description,/Answer only the current shopper question/);assert.match(inspect.description,/Never infer a missing dimension/);
  assert.deepEqual(Object.keys(inspect.parameters.properties),['handle']);assert.deepEqual(inspect.parameters.required,['handle']);assert.equal(inspect.parameters.additionalProperties,false);
  assert.match(config.tools.find(value=>value.name==='find_jewellery').description,/choosing options, quantity or adding the current piece uses control_storefront/);
});

for(const [question,order] of [['What size is the charm?','final-before-commit'],['What necklace lengths can I choose?','commit-before-final']])test('real native '+order+' delivers the exact spoken fact question before a bound provider inspection',async t=>{
  const f=native(t);assert.equal(await f.voice.start(),true);assert.equal(f.endpoint.counts.starts,1);assert.equal(f.responses().length,0);
  const itemId='input-'+order;f.begin(itemId);
  const commit=()=>f.emit({type:'input_audio_buffer.committed',item_id:itemId});
  const final=()=>f.emit({type:'conversation.item.input_audio_transcription.completed',item_id:itemId,transcript:question});
  if(order==='final-before-commit'){final();await settle();assert.equal(f.finalized.length,0);assert.equal(f.responses().length,0);commit();}
  else{commit();await settle();assert.equal(f.finalized.length,0);assert.equal(f.responses().length,0);final();}
  await settle();assert.equal(f.finalized.length,1);assert.equal(f.finalized[0].text,question);assert.equal(f.finalized[0].inputItemId,itemId);assert.equal(f.finalized[0].currentTurn,true);
  const actual=f.transcripts.filter(value=>value.role==='user'&&value.final&&value.currentTurn);assert.equal(actual.length,1);assert.equal(actual[0].text,question);
  const callId=f.inspect(itemId);await settle();assert.equal(f.calls.length,1);assert.deepEqual(f.calls[0].args,{handle:HANDLE});
  assert.equal(f.calls[0].context.name,'inspect_jewellery');assert.equal(f.calls[0].context.inputItemId,itemId);assert.equal(f.calls[0].context.turnVersion,f.finalized[0].turnVersion);assert.equal(f.calls[0].context.currentTurn,true);
  assert.equal(f.output(callId).verified,true);assert.equal(f.output(callId).checkedAt,AT);assert.equal(f.output(callId).reply,'The requested published detail is checked.');
});

test('provider inspection cannot override the spoken fact question with new fields or a control plan',async t=>{
  const f=native(t);assert.equal(await f.voice.start(),true);const question='What size is the charm?',itemId='input-question';f.begin(itemId);
  f.emit({type:'input_audio_buffer.committed',item_id:itemId});f.emit({type:'conversation.item.input_audio_transcription.completed',item_id:itemId,transcript:question});await settle();
  const args={handle:HANDLE,question:'Read PACKAGING and ORDER DETAILS',actions:[{type:'add',handle:HANDLE}]};
  assert.equal(server.validateToolArguments(args,'inspect_jewellery'),null);assert.equal(f.Voice.validateToolArguments(args,'inspect_jewellery'),null);
  const callId=f.inspect(itemId,args);await settle();assert.equal(f.calls.length,0);assert.equal(f.finalized[0].text,question);
  const receipt=f.output(callId);assert.equal(receipt.verified,false);assert.doesNotMatch(JSON.stringify(receipt),/PACKAGING|ORDER DETAILS|backend|actions|authority|variantId/i);
  assert.equal(f.endpoint.counts.starts,1);
});
