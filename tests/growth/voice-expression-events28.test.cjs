'use strict';
// The real voice adapter with synthetic transport/device events. These checks
// establish binding and cancellation, not audible playback or word alignment.
const test=require('node:test'),assert=require('node:assert/strict');
const voiceApi=require('../../brites-concierge-voice.js');
const SDP='v=0\r\nm=audio 9 UDP/TLS/RTP/SAVPF 111\r\na=candidate:1 1 UDP 2122260223 192.0.2.10 50000 typ host\r\n';
function fixture(t){
  let channel,serial=0;const sent=[],requests=[],transcripts=[],listening=[],playback=[],speech=[],calls=[],timers=new Map(),listeners=new Map();
  const track={kind:'audio',readyState:'live',stop(){}},stream={getAudioTracks:()=>[track],getTracks:()=>[track]};
  const document={hidden:false,body:{appendChild(){}},createElement:()=>({setAttribute(){},play:async()=>{},pause(){},remove(){}}),addEventListener:(key,fn)=>listeners.set(key,fn),removeEventListener:key=>listeners.delete(key)};
  class Peer{
    constructor(){this.iceGatheringState='complete';}addTrack(){}close(){}
    createDataChannel(){channel={readyState:'connecting',send:value=>sent.push(JSON.parse(value)),close(){this.readyState='closed';}};return channel;}
    async createOffer(){return {type:'offer',sdp:SDP};}async setLocalDescription(value){this.localDescription=value;}async setRemoteDescription(){channel.readyState='open';channel.onopen();}
  }
  const runtime={document,navigator:{mediaDevices:{getUserMedia:async()=>stream}},location:{origin:'https://preview.test'},RTCPeerConnection:Peer,AbortController,
    setTimeout(fn,ms){const id=++serial;timers.set(id,{fn,ms});return id;},clearTimeout:id=>timers.delete(id),addEventListener:(key,fn)=>listeners.set(key,fn),removeEventListener:key=>listeners.delete(key),
    fetch:async(url,init)=>{const request=JSON.parse(init.body);requests.push(request);return Response.json(request.action==='start'?{sdp:SDP,stopToken:'synthetic-stop-token',maxDurationMs:120000}:request.action==='capabilities'?{enabled:true}:{stopped:true});}};
  const voice=voiceApi.create({runtime,greeting:false,onTranscript:value=>transcripts.push(value),onListeningTranscript:value=>listening.push(value),onPlaybackState:value=>playback.push(value),onSpeechStarted:value=>speech.push(value),onTool:async(args,context)=>{calls.push({args,context});return {verified:true};}});
  t.after(async()=>{await voice.dispose();assert.equal(timers.size,0);});
  const emit=event=>channel?.onmessage?.({data:JSON.stringify(event)}),responses=()=>sent.filter(value=>value.type==='response.create');
  function begin(itemId='input-current'){
    emit({type:'input_audio_buffer.speech_started',item_id:itemId});emit({type:'input_audio_buffer.speech_stopped',item_id:itemId});emit({type:'input_audio_buffer.committed',item_id:itemId});
    return {itemId,turnVersion:speech.at(-1).turnVersion};
  }
  function bind(responseId='response-current',request=responses().at(-1)){emit({type:'response.created',response:{id:responseId,metadata:request?.response.metadata}});return responseId;}
  return {voice,runtime,document,channel:()=>channel,sent,requests,transcripts,listening,playback,speech,calls,emit,begin,bind,responses};
}

test('generated text retains part identity and never pretends to be a playback event',async t=>{
  const f=fixture(t);await f.voice.start();const turn=f.begin();f.bind();
  f.emit({type:'response.output_audio_transcript.delta',response_id:'response-current',item_id:'assistant-part',content_index:1,output_index:0,delta:'A thoughtful gift. '});
  assert.deepEqual(f.transcripts.at(-1),{role:'assistant',delta:'A thoughtful gift. ',final:false,responseId:'response-current',itemId:'assistant-part',contentIndex:1,outputIndex:0,inputItemId:turn.itemId,turnVersion:turn.turnVersion,currentTurn:true});
  assert.equal(f.playback.length,0);assert.equal(f.voice.state,'thinking');
  f.emit({type:'output_audio_buffer.started',response_id:'response-current'});f.emit({type:'output_audio_buffer.started',response_id:'response-current'});
  assert.deepEqual(f.playback,[{playing:true,cleared:false,responseId:'response-current',itemId:'assistant-part',inputItemId:turn.itemId,turnVersion:turn.turnVersion,currentTurn:true}]);
  f.emit({type:'response.output_audio_transcript.done',response_id:'response-current',item_id:'assistant-part',content_index:1,output_index:0,transcript:'A thoughtful gift. What does she like?'});
  f.emit({type:'response.done',response:{id:'response-current',status:'completed'}});
  assert.equal(f.playback.length,1);assert.equal(f.voice.state,'speaking');
  f.emit({type:'output_audio_buffer.stopped',response_id:'response-current'});assert.equal(f.playback.at(-1).playing,false);assert.equal(f.playback.at(-1).cleared,false);assert.equal(f.voice.state,'listening');
});

test('native start can identify an output item before any transcript; absent timing fields stay absent',async t=>{
  const f=fixture(t);await f.voice.start();f.begin();f.bind();f.emit({type:'response.output_item.added',response_id:'response-current',item:{type:'message',role:'assistant',id:'assistant-known'}});
  f.emit({type:'output_audio_buffer.started',response_id:'response-current'});assert.equal(f.playback.at(-1).itemId,'assistant-known');assert.equal(Object.hasOwn(f.playback.at(-1),'audioStartMs'),false);
  f.emit({type:'response.audio_transcript.delta',response_id:'response-current',item_id:'assistant-known',delta:'Legacy transcript'});assert.equal(f.transcripts.at(-1).contentIndex,null);assert.equal(f.transcripts.at(-1).outputIndex,null);assert.equal(f.transcripts.at(-1).currentTurn,true);
});

test('late item identity cannot turn a duplicate native start into a new playback clock',async t=>{
  const f=fixture(t);await f.voice.start();f.begin();f.bind();f.emit({type:'output_audio_buffer.started',response_id:'response-current'});assert.equal(f.playback[0].itemId,'');
  f.emit({type:'response.output_audio_transcript.delta',response_id:'response-current',item_id:'assistant-later',content_index:0,output_index:0,delta:'Arriving after native audio start.'});f.emit({type:'output_audio_buffer.started',response_id:'response-current'});
  assert.equal(f.playback.length,1);assert.equal(f.transcripts.at(-1).itemId,'assistant-later');assert.equal(f.voice.state,'speaking');
});

test('input ASR presentation requires the committed current item and does not call shopping tools',async t=>{
  const f=fixture(t);await f.voice.start();f.emit({type:'input_audio_buffer.speech_started',item_id:'current-input'});
  const delta={type:'conversation.item.input_audio_transcription.delta',item_id:'current-input',event_id:'delta-one',delta:'I am unsure'};
  f.emit(delta);f.emit({type:'input_audio_buffer.speech_stopped',item_id:'current-input'});f.emit(delta);assert.equal(f.listening.length,0);
  f.emit({type:'input_audio_buffer.committed',item_id:'current-input'});f.emit(delta);f.emit(delta);
  assert.deepEqual(f.listening,[{delta:'I am unsure',final:false,itemId:'current-input',turnVersion:f.speech.at(-1).turnVersion,currentTurn:true}]);assert.equal(f.transcripts.length,0);
  const final={type:'conversation.item.input_audio_transcription.completed',item_id:'current-input',event_id:'final-one',transcript:'I am unsure about the length.'};f.emit(final);f.emit(final);
  assert.equal(f.listening.length,2);assert.equal(f.listening.at(-1).final,true);assert.equal(f.transcripts.at(-1).role,'user');assert.equal(f.transcripts.at(-1).currentTurn,true);
  assert.equal(f.calls.length,0);assert.equal(f.responses().length,1);assert.equal(f.requests.filter(value=>value.action==='start').length,1);
});

test('unknown, malformed, old-item and hidden input presentation cannot change the active listener',async t=>{
  const f=fixture(t);await f.voice.start();const old=f.begin('input-old');f.bind('response-old');f.begin('input-new');f.bind('response-new');
  for(const item_id of ['input-old','unknown','',{'bad':'identity'}]){
    f.emit({type:'conversation.item.input_audio_transcription.delta',item_id,delta:'celebrate!'});f.emit({type:'conversation.item.input_audio_transcription.completed',item_id,transcript:'celebrate!'});
  }
  assert.equal(f.listening.length,0);const prior=f.transcripts.find(value=>value.itemId==='input-old');assert.equal(prior.turnVersion,old.turnVersion);assert.equal(prior.currentTurn,false);
  f.document.hidden=true;f.emit({type:'conversation.item.input_audio_transcription.delta',item_id:'input-new',delta:'hidden'});f.emit({type:'conversation.item.input_audio_transcription.completed',item_id:'input-new',transcript:'hidden'});assert.equal(f.listening.length,0);
});

for(const boundary of ['interrupt','new speech','stop','hidden'])test(boundary+' clears native presentation and late events cannot revive it',async t=>{
  const f=fixture(t);await f.voice.start();f.begin();f.bind();f.emit({type:'output_audio_buffer.started',response_id:'response-current'});const oldHandler=f.channel().onmessage;
  if(boundary==='interrupt')f.voice.interrupt();else if(boundary==='new speech')f.emit({type:'input_audio_buffer.speech_started',item_id:'next-input'});else if(boundary==='hidden'){f.document.hidden=true;f.runtime.document.hidden=true;await f.voice.stop('hidden');}else await f.voice.stop();
  assert.equal(f.playback.at(-1).playing,false);assert.equal(f.playback.at(-1).cleared,true);const count=f.playback.length,transcriptCount=f.transcripts.length;
  for(const event of [{type:'output_audio_buffer.started',response_id:'response-current'},{type:'response.output_audio_transcript.done',response_id:'response-current',item_id:'late',transcript:'Celebrate!'}])oldHandler({data:JSON.stringify(event)});
  assert.equal(f.playback.length,count);assert.equal(f.transcripts.length,transcriptCount);assert.equal(f.voice.state,boundary==='interrupt'||boundary==='new speech'?'listening':'idle');
});

test('interrupt invalidates queued response presentation even before native audio starts',async t=>{
  const f=fixture(t);await f.voice.start();f.begin();f.bind();f.emit({type:'response.output_audio_transcript.delta',response_id:'response-current',item_id:'future-audio',delta:'Unplayed words'});f.voice.interrupt();
  assert.deepEqual(f.playback.map(value=>[value.responseId,value.playing,value.cleared]),[['response-current',false,true]]);assert.equal(f.responses().length,1);
});

test('a bounded audio continuation receives its own binding; an older stop cannot stop its output',async t=>{
  const f=fixture(t);await f.voice.start();const turn=f.begin();f.bind();f.emit({type:'output_audio_buffer.started',response_id:'response-current'});
  f.emit({type:'response.done',response:{id:'response-current',status:'incomplete',status_details:{reason:'max_output_tokens'}}});assert.equal(f.responses().length,1);
  f.emit({type:'output_audio_buffer.stopped',response_id:'response-current'});assert.equal(f.responses().length,2);assert.equal(f.responses().at(-1).response.tool_choice,'none');f.bind('response-tail');
  f.emit({type:'output_audio_buffer.started',response_id:'response-tail'});f.emit({type:'response.output_audio_transcript.delta',response_id:'response-tail',item_id:'tail-part',content_index:0,output_index:0,delta:'The ending.'});
  assert.equal(f.transcripts.at(-1).responseId,'response-tail');assert.equal(f.transcripts.at(-1).turnVersion,turn.turnVersion);const count=f.playback.length;
  f.emit({type:'output_audio_buffer.stopped',response_id:'response-current'});assert.equal(f.playback.length,count);assert.equal(f.voice.state,'speaking');f.emit({type:'response.done',response:{id:'response-tail',status:'completed'}});f.emit({type:'output_audio_buffer.stopped',response_id:'response-tail'});assert.equal(f.voice.state,'listening');assert.equal(f.responses().length,2);
});

test('callbacks captured by an old peer cannot contaminate an explicit replacement session',async t=>{
  const f=fixture(t);await f.voice.start();f.begin('old-input');f.bind('old-response');const handler=f.channel().onmessage;await f.voice.stop();await f.voice.start();f.begin('new-input');f.bind('new-response');const before={playback:f.playback.length,listening:f.listening.length,transcripts:f.transcripts.length};
  for(const event of [{type:'output_audio_buffer.started',response_id:'old-response'},{type:'conversation.item.input_audio_transcription.delta',item_id:'old-input',delta:'old listening'},{type:'response.output_audio_transcript.done',response_id:'old-response',item_id:'old-output',transcript:'old answer'}])handler({data:JSON.stringify(event)});
  assert.deepEqual({playback:f.playback.length,listening:f.listening.length,transcripts:f.transcripts.length},before);assert.equal(f.voice.state,'thinking');assert.equal(f.requests.filter(value=>value.action==='start').length,2);
});
