'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const native=require('../../brites-concierge-voice.js');
const context={pageKind:'product',currentHandle:'compass-necklace',selectedHandle:'compass-necklace',displayedPieces:[{id:'gid://shopify/Product/1',handle:'compass-necklace',title:'Compass Necklace'}]};
test('public context projects only bounded public identity hints, excluding facts, URLs and permissions',()=>{
 const result=native.publicContext({...context,token:'secret',permissions:['cart'],displayedPieces:[{...context.displayedPieces[0],price:99,available:true,url:'https://private.invalid',title:'Compass\u0000 Necklace'},...Array(9).fill(context.displayedPieces[0])]});
 assert.equal(result.displayedPieces.length,6);assert.equal(result.displayedPieces[0].title,'Compass  Necklace');assert.doesNotMatch(JSON.stringify(result),/secret|private|price|available|permissions/);assert.equal(native.publicContext({...context,currentHandle:'https://evil.invalid',selectedHandle:'unlisted'}).selectedHandle,'');assert.equal(native.publicContext({...context,currentHandle:'https://evil.invalid'}).currentHandle,'');assert.equal(native.publicContext(null),null);
});
function fixture(t){
 const sent=[],sdp='v=0\r\nm=audio 9 UDP/TLS/RTP/SAVPF 111\r\na=candidate:1 1 UDP 2122260223 192.0.2.10 50000 typ host\r\n';let dc;
 class Peer{constructor(){this.iceGatheringState='complete';}addTrack(){}createDataChannel(){dc={readyState:'connecting',send:v=>sent.push(JSON.parse(v)),close(){}};return dc;}async createOffer(){return {type:'offer',sdp};}async setLocalDescription(v){this.localDescription=v;}async setRemoteDescription(){setImmediate(()=>{dc.readyState='open';dc.onopen();});}close(){}}
 const stream={getTracks:()=>[{stop(){}}],getAudioTracks:()=>[{stop(){}}]},document={hidden:false,body:{appendChild(){}},createElement:()=>({setAttribute(){},pause(){},remove(){},play:async()=>{}}),addEventListener(){},removeEventListener(){}};
 const runtime={document,navigator:{mediaDevices:{getUserMedia:async()=>stream}},location:{origin:'https://preview.example'},RTCPeerConnection:Peer,AbortController,setTimeout,clearTimeout,addEventListener(){},removeEventListener(){},fetch:async(url,init)=>Response.json(JSON.parse(init.body).action==='capabilities'?{enabled:true}:JSON.parse(init.body).action==='start'?{sdp,stopToken:'fixture-only-token',maxDurationMs:120000}:{stopped:true})};
 const voice=native.create({runtime,greeting:false,getContext:()=>context});t.after(()=>voice.dispose());return {voice,sent,document};
}
test('context startup and updates never create responses or shopper action authority',async t=>{
 const h=fixture(t);assert.equal(h.voice.updateContext(context),false);await h.voice.start();assert.equal(h.sent.length,1);assert.equal(h.sent[0].type,'conversation.item.create');assert.match(h.sent[0].item.content[0].text,/not a shopper utterance/);assert.equal(h.voice.updateContext(context),false);assert.equal(h.voice.updateContext({...context,selectedHandle:''}),true);assert.ok(h.sent.every(e=>e.type!=='response.create'));assert.doesNotMatch(JSON.stringify(h.sent),/brites_voice_request|brites_input_item/);h.document.hidden=true;assert.equal(h.voice.updateContext(context),false);h.document.hidden=false;await h.voice.stop();assert.equal(h.voice.updateContext(context),false);await h.voice.start();assert.equal(h.sent.filter(e=>e.type==='conversation.item.create').length,3);
});
