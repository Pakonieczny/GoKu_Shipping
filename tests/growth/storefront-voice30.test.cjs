'use strict';
// Actual native adapter, synthetic transport only: no inference or microphone.
const test=require('node:test'),assert=require('node:assert/strict');
const client=require('../../brites-concierge-voice.js'),server=require('../../netlify/functions/_britesConciergeVoice.js');
const piece={id:'gid://shopify/Product/3100',handle:'compass-necklace',title:'Compass Necklace'};
const SDP='v=0\r\nm=audio 9 UDP/TLS/RTP/SAVPF 111\r\na=candidate:1 1 UDP 2122260223 192.0.2.10 50000 typ host\r\n';
const settle=()=>new Promise(resolve=>setImmediate(resolve));
function fixture(t,{onTool=async()=>({ok:true,live:true}),getContext}={}){
  let channel,serial=0;const sent=[],calls=[],timers=new Map();
  const track={stop(){}},stream={getTracks:()=>[track],getAudioTracks:()=>[track]};
  const document={hidden:false,body:{appendChild(){}},createElement:()=>({setAttribute(){},play:async()=>{},pause(){},remove(){}}),addEventListener(){},removeEventListener(){}};
  class Peer{constructor(){this.iceGatheringState='complete';}addTrack(){}close(){}createDataChannel(){channel={readyState:'connecting',send:v=>sent.push(JSON.parse(v)),close(){}};return channel;}async createOffer(){return {type:'offer',sdp:SDP};}async setLocalDescription(value){this.localDescription=value;}async setRemoteDescription(){channel.readyState='open';channel.onopen();}}
  const runtime={document,navigator:{mediaDevices:{getUserMedia:async()=>stream}},location:{origin:'https://preview.test'},RTCPeerConnection:Peer,AbortController,addEventListener(){},removeEventListener(){},setTimeout(fn,ms){const id=++serial;timers.set(id,{fn,ms});return id;},clearTimeout:id=>timers.delete(id),fetch:async(url,init)=>Response.json(JSON.parse(init.body).action==='capabilities'?{enabled:true}:JSON.parse(init.body).action==='start'?{sdp:SDP,stopToken:'synthetic-token',maxDurationMs:120000}:{stopped:true})};
  const voice=client.create({runtime,greeting:false,getContext,onTool:async(args,context)=>{calls.push({args,context});return onTool(args,context);}});
  t.after(()=>voice.dispose());
  const emit=event=>channel.onmessage({data:JSON.stringify(event)}),responses=()=>sent.filter(e=>e.type==='response.create');
  function begin(item='input-current'){emit({type:'input_audio_buffer.speech_started',item_id:item});emit({type:'input_audio_buffer.speech_stopped',item_id:item});emit({type:'input_audio_buffer.committed',item_id:item});}
  function bind(id='response-current',metadata=responses().at(-1)?.response.metadata){emit({type:'response.created',response:{id,metadata}});}
  function tool(name,args,id='response-current',call_id='tool-current'){emit({type:'response.function_call_arguments.done',response_id:id,call_id,name,arguments:JSON.stringify(args)});}
  return {voice,sent,calls,emit,begin,bind,tool,responses,document};
}
test('browser and server reject extra authority fields and agree on canonical controls',()=>{
  const valid=[{type:'search',query:' sterling silver '},{type:'sort',sort:'price-asc'},{type:'filter',filter:'earrings'},{type:'open',handle:piece.handle},{type:'zoom',handle:piece.handle},{type:'highlight',handle:piece.handle,section:'price'},{type:'scroll',section:'shipping'},{type:'bag'},{type:'checkout'},{type:'gift',section:'gifts'},{type:'customize',section:'customize',handle:piece.handle}];
  const invalid=[{},[],null,{type:'buy'},{type:'search'},{type:'search',query:'x'.repeat(181)},{type:'search',query:'x\u0000y'},{type:'sort',sort:'random'},{type:'filter',filter:'solid-gold'},{type:'open',handle:'https://other.invalid'},{type:'zoom'},{type:'highlight',section:'body'},{type:'scroll',section:'price',selector:'#pay'},{type:'checkout',confirm:true},{type:'bag',variantId:'123'},{type:'open',handle:piece.handle,url:'https://britesjewelry.com'}];
  for(const value of valid){assert.deepEqual(client.validateToolArguments(value,'control_storefront'),server.validateToolArguments(value,'control_storefront'));assert.ok(client.validateToolArguments(value,'control_storefront'));}
  for(const value of invalid)for(const implementation of [client,server])assert.equal(implementation.validateToolArguments(value,'control_storefront'),null,JSON.stringify(value));
  for(const implementation of [client,server]){assert.deepEqual(implementation.validateToolArguments({},'read_storefront_services'),{});assert.equal(implementation.validateToolArguments({url:'https://other.invalid'},'read_storefront_services'),null);}
});
test('visible context is immediate bounded identity data, never prices, permissions or private fields',()=>{
  const projected=client.publicContext({pageKind:'collection',currentHandle:'',focusedHandle:piece.handle,visiblePieces:Array.from({length:30},()=>({...piece,price:999,available:true,url:'https://private.invalid',instructions:'private instructions'})),displayedPieces:[],contextRevision:77,search:'silver',sort:'price-asc',filter:'necklaces',loading:false,activeSection:'catalogue',token:'private secret',permissions:['buy']});
  assert.equal(projected.visiblePieces.length,24);assert.equal(projected.focusedHandle,piece.handle);assert.equal(projected.contextRevision,77);assert.deepEqual(projected.visiblePieces[0],piece);assert.doesNotMatch(JSON.stringify(projected),/999|private|instructions|permissions|secret/);
  const malformed=client.publicContext({pageKind:'payment',currentHandle:'javascript:evil',focusedHandle:'unlisted',visiblePieces:[{...piece,id:'bad'}],contextRevision:-1,sort:'danger',filter:'danger',activeSection:'payment'});
  assert.equal(malformed.pageKind,'other');assert.equal(malformed.focusedHandle,'');assert.equal(malformed.currentHandle,'');assert.deepEqual(malformed.visiblePieces,[]);assert.equal(Object.hasOwn(malformed,'contextRevision'),false);assert.equal(Object.hasOwn(malformed,'sort'),false);
});
test('native storefront action is bound to exact committed item and echoed client response',async t=>{
  const f=fixture(t);await f.voice.start();f.begin();f.bind();f.tool('control_storefront',{type:'sort',sort:'price-asc'});f.tool('control_storefront',{type:'sort',sort:'price-asc'});await settle();
  assert.equal(f.responses().length,1);f.emit({type:'response.done',response:{id:'response-current',status:'completed'}});
  assert.equal(f.calls.length,1);assert.equal(f.calls[0].context.inputItemId,'input-current');assert.equal(f.calls[0].context.responseId,'response-current');assert.equal(f.calls[0].context.currentTurn,true);assert.equal(f.responses().at(-1).response.tool_choice,'none','control cannot chain another website change');
});
test('missing metadata, uncommitted and stale spoken turns cannot reach native controls',async t=>{
  const f=fixture(t);await f.voice.start();f.begin('old-input');f.bind('old-response');f.begin('new-input');f.bind('unbound-response',{});f.tool('control_storefront',{type:'bag'},'unbound-response','unbound');f.tool('control_storefront',{type:'bag'},'old-response','old');await settle();assert.equal(f.calls.length,0);
  f.emit({type:'input_audio_buffer.speech_started',item_id:'pending-input'});f.bind('pending-response',f.responses().at(-1).response.metadata);f.tool('control_storefront',{type:'checkout'},'pending-response','pending');await settle();assert.equal(f.calls.length,0);
});
test('services are read-only and leave a bounded checked continuation available',async t=>{
  const f=fixture(t,{onTool:async()=>({verified:false,readCompleted:true,schema:1,checkedAt:1791349200000,guidance:{production:{needsConfirmation:true}},offers:[]})});await f.voice.start();f.begin();f.bind();f.tool('read_storefront_services',{});await settle();f.emit({type:'response.done',response:{id:'response-current',status:'completed'}});assert.equal(f.calls.length,1);assert.equal(f.calls[0].context.name,'read_storefront_services');assert.equal(f.responses().at(-1).response.tool_choice,'auto');
});
test('a newer spoken turn aborts pending controls and does not announce false success',async t=>{
  let resolve;const pending=new Promise(done=>resolve=done),f=fixture(t,{onTool:()=>pending});await f.voice.start();f.begin('first-input');f.bind('first-response');f.tool('control_storefront',{type:'open',handle:piece.handle},'first-response');await settle();assert.equal(f.calls.length,1);
  f.begin('next-input');assert.equal(f.calls[0].context.signal.aborted,true);resolve({ok:true,live:true,message:'Opened'});await settle();const outputs=f.sent.filter(e=>e.item?.type==='function_call_output');assert.ok(outputs.length);assert.match(outputs.at(-1).item.output,/cancelled/);assert.doesNotMatch(outputs.at(-1).item.output,/Opened/);
});
test('session config advertises mock controls and attributed policy reads without live checkout permission',()=>{
  const tools=server.sessionConfig({}).tools;assert.ok(tools.some(t=>t.name==='control_storefront'));assert.ok(tools.some(t=>t.name==='read_storefront_services'));assert.ok(tools.every(t=>t.parameters.additionalProperties===false));assert.match(server.instructions,/actual finalized shopper words/);assert.match(server.instructions,/explicitly labelled test checkout/);assert.match(server.instructions,/discrepancy/);assert.match(server.instructions,/Production time is not delivery time/);assert.doesNotMatch(JSON.stringify(tools),/OPENAI_API_KEY/);
});

test('local hover context is sent only when an actual response requests the newest snapshot',async t=>{
  let page={pageKind:'collection',visiblePieces:[piece],focusedHandle:piece.handle,contextRevision:1,search:'silver'};const f=fixture(t,{getContext:()=>page});await f.voice.start();
  const before=f.sent.filter(e=>e.item?.content?.[0]?.text?.startsWith('Public website UI context')).length;page={...page,contextRevision:2,search:'bunny'};assert.equal(f.sent.filter(e=>e.item?.content?.[0]?.text?.startsWith('Public website UI context')).length,before);
  f.begin();const contexts=f.sent.filter(e=>e.item?.content?.[0]?.text?.startsWith('Public website UI context'));assert.equal(contexts.length,before+1);assert.match(contexts.at(-1).item.content[0].text,/"contextRevision":2/);assert.match(contexts.at(-1).item.content[0].text,/"search":"bunny"/);assert.equal(f.responses().length,1);assert.equal(f.calls.length,0);
});
