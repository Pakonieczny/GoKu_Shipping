'use strict';
// Real widget/adapter, synthetic media and catalogue. No provider calls; these
// regressions cannot certify microphone, WebRTC playback, GPU or latency.
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs');
const {JSDOM}=require('jsdom');
const native=require('../../brites-concierge-voice.js');
const widget=fs.readFileSync(require.resolve('../../brites-concierge.js'),'utf8');
const helper=fs.readFileSync(require.resolve('../../brites-concierge-voice-actions.js'),'utf8');
const copy=value=>JSON.parse(JSON.stringify(value)),tick=()=>new Promise(resolve=>setImmediate(resolve));
async function settle(){await tick();await tick();}
function deferred(){let resolve,reject;const promise=new Promise((a,b)=>{resolve=a;reject=b;});return {promise,resolve,reject};}
function piece(id=1){return {id:'gid://shopify/Product/'+id,handle:id===1?'compass-necklace':'bunny-necklace',url:'https://britesjewelry.com/products/'+(id===1?'compass-necklace':'bunny-necklace'),title:id===1?'Compass Necklace':'Bunny Necklace',currency:'USD',type:'Necklace',variantsComplete:true,minPrice:54,suggestedVariantId:'gid://shopify/ProductVariant/'+id+'01',variants:[{id:'gid://shopify/ProductVariant/'+id+'01',numericId:id+'01',title:'Sterling Silver / 18 inch / None',price:54,available:true,options:[{name:'Metal',value:'Sterling Silver'},{name:'Length',value:'18 inch'},{name:'Engraving',value:'None'}]}]};}
function fixture(t){
  const dom=new JSDOM('<!doctype html><body></body>',{url:'https://preview.example/concierge-sandbox.html',runScripts:'outside-only',pretendToBeVisual:true}),w=dom.window,d=w.document;
  const script=d.createElement('script');script.src='/brites-concierge.js';script.dataset.sandbox='true';Object.defineProperty(d,'currentScript',{get:()=>script});Object.defineProperty(d,'hidden',{get:()=>false});
  w.HTMLElement.prototype.scrollIntoView=function(){};w.eval(helper);
  w.BritesConciergeAvatar={create:()=>({setState(){},setEmotion(){},setVisible(){},setLevel(){},setPaused(){},triggerGreeting(){}})};
  const clients=[],reads=[];let live=piece(),answer={live:true,reply:'Current selection.',preferences:{},products:[piece()],meanings:[]},readGate=null,answerGate=null;
  w.BritesConciergeVoice={create(config){const client={config,starts:0,stops:0,disposed:false,async start(){this.starts++;if(this.disposed)throw Error('Voice adapter is closed.');return true;},async stop(){this.stops++;},interrupt(){},async dispose(){this.disposed=true;}};clients.push(client);return client;}};
  w.fetch=async(url,init={})=>{if(String(url).includes('/api/growth/product?')){reads.push(init.signal);const value=readGate?await readGate.promise:{product:copy(live),live:true};return {ok:true,json:async()=>value};}if(String(url).endsWith('/api/concierge'))return {ok:true,json:async()=>answerGate?answerGate.promise:copy(answer)};return {ok:true,json:async()=>({})};};w.eval(widget);
  const root=d.querySelector('brites-concierge').shadowRoot,button=text=>[...root.querySelectorAll('button')].find(x=>x.textContent.trim()===text),config=()=>clients.at(-1).config;
  let version=0;function speech(text,final=true){const turn={inputItemId:'input-'+(++version),turnVersion:version,currentTurn:true};config().onSpeechStarted({itemId:turn.inputItemId,turnVersion:version,reason:'speech'});if(final)transcript(text,turn);return turn;}
  function transcript(text,turn){config().onTranscript({role:'user',itemId:turn.inputItemId,turnVersion:turn.turnVersion,currentTurn:true,text,final:true});}
  function tool(name,args,context={}){return config().onTool(args,{name,...context});}
  async function activate(){button('Talk to me').click();await settle();const loader=root.querySelector('script[src$="brites-concierge-voice.js"]');loader?.dispatchEvent(new w.Event('load'));await settle();}
  async function ready(){w.BritesConcierge.open();await activate();await tool('find_jewellery',{message:'A graduation necklace'});}
  t.after(()=>{w.BritesConcierge.close();w.close();});
  return {w,root,clients,reads,button,config,speech,transcript,tool,activate,ready,setReadGate:x=>{readGate=x;},setAnswerGate:x=>{answerGate=x;},setLive:x=>{live=copy(x);},saved:()=>JSON.parse(w.sessionStorage.getItem('brites-concierge-v1'))};
}
test('matching final ASR preserves a pending same-item same-turn read-only inspection',async t=>{
  const h=fixture(t);await h.ready();const context=h.speech('',false),gate=deferred();h.setReadGate(gate);const checked=h.tool('inspect_jewellery',{handle:piece().handle},context);await settle();
  h.transcript('What options does the Compass Necklace have?',context);assert.equal(h.reads.at(-1).aborted,false);gate.resolve({product:piece(),live:true});const result=await checked;assert.equal(result.live,true);assert.equal(result.product.id,piece().id);assert.equal(h.root.querySelector('select'),null);
});
for(const boundary of ['new speech','manual selection','End voice'])test(boundary+' still cancels pending read-only inspection after matching ASR',async t=>{
  const h=fixture(t);await h.ready();const context=h.speech('',false),gate=deferred();h.setReadGate(gate);const checked=h.tool('inspect_jewellery',{handle:piece().handle},context);await settle();h.transcript('Show options for the Compass Necklace.',context);
  if(boundary==='new speech')h.speech('',false);else if(boundary==='manual selection')h.button('Choose options').click();else h.button('End voice').click();assert.equal(h.reads.at(-1).aborted,true);assert.ok((await checked).error);gate.resolve({product:piece(),live:true});await settle();
});
for(const arrival of ['during search','after search'])test('ASR '+arrival+' cannot rebind an earlier ordinal to a newly discovered selection',async t=>{
  const h=fixture(t);await h.ready();const context=h.speech('',false),gate=deferred();h.setAnswerGate(gate);const discovery=h.tool('find_jewellery',{message:'Show options for the first one.'},context);await settle();
  if(arrival==='during search')h.transcript('Show options for the first one.',context);gate.resolve({live:true,reply:'Another selection.',preferences:{},products:[piece(2)],meanings:[]});await discovery;h.setAnswerGate(null);h.setLive(piece(2));
  if(arrival==='after search')h.transcript('Show options for the first one.',context);const rejected=await h.tool('prepare_jewellery_action',{handle:piece(2).handle,action:'options'},context);assert.ok(rejected.error);assert.equal(h.root.querySelector('select'),null);assert.equal(h.w.sessionStorage.getItem('brites-sandbox-cart'),null);
  const fresh=h.speech('Show options for the first one.');const prepared=await h.tool('prepare_jewellery_action',{handle:piece(2).handle,action:'options'},fresh);assert.equal(prepared.prepared,true);assert.equal(prepared.product.id,piece(2).id);assert.ok(h.root.querySelector('select'));assert.equal(h.w.sessionStorage.getItem('brites-sandbox-cart'),null);
});
test('a productless policy check preserves the unchanged displayed selection without resurrecting prepared controls',async t=>{
  const h=fixture(t);await h.ready();const context=h.speech('Open the Compass Necklace.');await h.tool('prepare_jewellery_action',{handle:piece().handle,action:'view'},context);assert.ok(h.button('Open this piece'));
  const next=h.speech('',false),gate=deferred();h.setAnswerGate(gate);const lookup=h.tool('find_jewellery',{message:'Show the options for the Compass Necklace.'},next);await settle();gate.resolve({live:true,reply:'Policy checked.',preferences:{},products:[],meanings:[],policyOnly:true,policyKnowledge:{status:'verified'}});await lookup;h.transcript('Show options for the Compass Necklace.',next);
  assert.equal(h.button('Open this piece'),undefined);assert.equal((await h.tool('prepare_jewellery_action',{handle:piece().handle,action:'options'},next)).prepared,true);
});
test('BFCache return preserves harmless history/cards, recreates the disposed adapter only on explicit Talk, and drops prior authority',async t=>{
  const h=fixture(t);await h.ready();const context=h.speech('Open the Compass Necklace.');await h.tool('prepare_jewellery_action',{handle:piece().handle,action:'view'},context);const old=h.clients[0],history=h.saved().history;
  h.w.dispatchEvent(new h.w.PageTransitionEvent('pagehide',{persisted:true}));await settle();h.w.dispatchEvent(new h.w.PageTransitionEvent('pageshow',{persisted:true}));await settle();assert.equal(old.disposed,true);assert.equal(h.clients.length,1);assert.equal(old.starts,1);assert.deepEqual(h.saved().history,history);assert.equal(h.root.querySelectorAll('.card').length,1);assert.equal(h.button('Open this piece'),undefined);
  await h.activate();assert.equal(h.clients.length,2);assert.equal(h.clients[1].starts,1);assert.ok(h.button('End voice'));assert.ok((await h.tool('prepare_jewellery_action',{handle:piece().handle,action:'view'},context)).error);
});
test('obsolete disposed adapter callbacks cannot close or contaminate a new explicit session',async t=>{
  const h=fixture(t);await h.ready();const old=h.clients[0];h.w.dispatchEvent(new h.w.PageTransitionEvent('pagehide',{persisted:true}));await settle();h.w.dispatchEvent(new h.w.PageTransitionEvent('pageshow',{persisted:true}));await h.activate();const before=h.saved().history,caption=h.root.querySelector('.caption-text').textContent;
  old.config.onState('speaking');old.config.onLevel({output:1});old.config.onTranscript({role:'assistant',itemId:'stale',text:'STALE',final:true});old.config.onSpeechStarted({itemId:'stale',turnVersion:99});old.config.onError('Your browser paused OpenAI audio. End voice and start again to allow playback.');old.config.onStopped();assert.ok((await old.config.onTool({message:'STALE'},{name:'find_jewellery'})).error);
  assert.ok(h.button('End voice'));assert.equal(h.clients[1].stops,0);assert.deepEqual(h.saved().history,before);assert.equal(h.root.querySelector('.caption-text').textContent,caption);assert.equal(h.root.querySelector('.panel').dataset.voice,'connecting');
});
test('a voice script loaded after pagehide cannot create or replace an adapter in a newer session',async t=>{
  const h=fixture(t);h.w.BritesConcierge.open();h.button('Talk to me').click();await settle();const oldLoader=h.root.querySelector('script[src$="brites-concierge-voice.js"]');assert.ok(oldLoader);
  h.w.dispatchEvent(new h.w.PageTransitionEvent('pagehide',{persisted:true}));await settle();h.w.dispatchEvent(new h.w.PageTransitionEvent('pageshow',{persisted:true}));h.button('Talk to me').click();await settle();const loaders=[...h.root.querySelectorAll('script[src$="brites-concierge-voice.js"]')],newLoader=loaders.find(x=>x!==oldLoader);assert.ok(newLoader);oldLoader.dispatchEvent(new h.w.Event('load'));await settle();assert.equal(h.clients.length,0);newLoader.dispatchEvent(new h.w.Event('load'));await settle();assert.equal(h.clients.length,1);assert.equal(h.clients[0].starts,1);assert.ok(h.button('End voice'));
});
function nativeFixture(t){
  const sdp='v=0\r\nm=audio 9 UDP/TLS/RTP/SAVPF 111\r\na=candidate:fixture 1 udp 123 192.0.2.1 49152 typ host\r\n',errors=[],audios=[];let pc,dc;
  class Peer{constructor(){pc=this;this.iceGatheringState='complete';}addTrack(){}createDataChannel(){dc={readyState:'connecting',send(){},close(){}};return dc;}async createOffer(){return {type:'offer',sdp};}async setLocalDescription(v){this.localDescription=v;}async setRemoteDescription(){setImmediate(()=>{dc.readyState='open';dc.onopen();});}close(){}}
  const stream={getTracks:()=>[{stop(){}}],getAudioTracks:()=>[{stop(){}}]},document={hidden:false,body:{appendChild(){}},createElement(){const playback=deferred(),a={setAttribute(){},pause(){},remove(){},srcObject:null,play:()=>playback.promise,playback};audios.push(a);return a;},addEventListener(){},removeEventListener(){}};
  const runtime={document,navigator:{mediaDevices:{getUserMedia:async()=>stream}},location:{origin:'https://preview.example'},RTCPeerConnection:Peer,AbortController,setTimeout,clearTimeout,addEventListener(){},removeEventListener(){},fetch:async(url,init)=>{const action=JSON.parse(init.body).action;return Response.json(action==='capabilities'?{enabled:true}:action==='start'?{sdp,stopToken:'test-only-token',maxDurationMs:120000}:{stopped:true});}};
  const voice=native.create({runtime,greeting:false,onError:x=>errors.push(x)});t.after(()=>voice.dispose());return {voice,errors,audios,peer:()=>pc,channel:()=>dc,track:()=>pc.ontrack({streams:[stream]})};
}
for(const boundary of ['stop','restart','dispose'])test('old playback rejection after '+boundary+' is suppressed by exact session/audio identity',async t=>{
  const h=nativeFixture(t);await h.voice.start();h.track();const old=h.audios[0];if(boundary==='dispose')await h.voice.dispose();else await h.voice.stop();if(boundary==='restart')await h.voice.start();old.playback.reject(Error('Old playback failure'));await settle();assert.deepEqual(h.errors,[]);assert.equal(h.voice.state,boundary==='restart'?'listening':'idle');
});
test('current playback rejection still supplies the fixed helpful local failure',async t=>{
  const h=nativeFixture(t);await h.voice.start();h.track();h.audios[0].playback.reject(Error('Provider/private detail'));await settle();assert.deepEqual(h.errors,['Your browser paused OpenAI audio. End voice and start again to allow playback.']);
});

for(const event of ['channel error','channel close','peer failure'])test('delayed old '+event+' cannot stop a restarted native session',async t=>{
  const h=nativeFixture(t);await h.voice.start();const oldPeer=h.peer(),oldChannel=h.channel(),callback=event==='channel error'?oldChannel.onerror:event==='channel close'?oldChannel.onclose:oldPeer.onconnectionstatechange;await h.voice.stop();await h.voice.start();oldPeer.connectionState='failed';callback();await settle();assert.deepEqual(h.errors,[]);assert.equal(h.voice.state,'listening');
});
for(const event of ['channel error','channel close','peer failure'])test('current '+event+' still stops media and returns a fixed safe failure',async t=>{
  const h=nativeFixture(t);await h.voice.start();if(event==='channel error')h.channel().onerror();else if(event==='channel close')h.channel().onclose();else {h.peer().connectionState='failed';h.peer().onconnectionstatechange();}await settle();assert.equal(h.voice.state,'idle');assert.equal(h.errors.length,1);assert.doesNotMatch(h.errors[0],/token|private|provider/i);
});
