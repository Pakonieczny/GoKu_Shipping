'use strict';
// Real voice client, mounted widget, bridge and sandbox controls. The microphone,
// peer, HTTP and provider events are simulated; this cannot certify physical ASR
// or audible speech. Commands enter only as native finalized audio events.
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs');
const {JSDOM,VirtualConsole}=require('jsdom');
const Voice=require('../../brites-concierge-voice.js');
const files=['concierge-sandbox.html','brites-catalogue-intents.js','brites-concierge-shopping-guide.js','brites-storefront-bridge.js','concierge-sandbox.js','brites-concierge.js'];
const sources=Object.fromEntries(files.map(name=>[name,fs.readFileSync(require.resolve('../../'+name),'utf8')]));
const clone=value=>JSON.parse(JSON.stringify(value));
const settle=async()=>{for(let n=0;n<12;n++)await new Promise(setImmediate);};
const SDP='v=0\r\nm=audio 9 UDP/TLS/RTP/SAVPF 111\r\na=candidate:1 1 UDP 2122260223 192.0.2.46 50000 typ host\r\n';
const handle='lowercase-initial-necklace',title='Lowercase Initial Necklace';
const categories=['regular-necklaces','beady-necklaces','stud-earrings','hoop-earrings','charm-only'];
function product(index){
  const materials=['Sterling Silver','14k Gold Filled','14k Solid Gold'],lengths=['16 inch','18 inch'],engraving=['None','Engraved'];
  const options=[{name:'Metal Choice',values:materials},{name:'Chain Length',values:lengths},{name:'Engraving',values:engraving}],variants=[];let serial=0;
  for(const [m,metal] of materials.entries())for(const [l,length] of lengths.entries())for(const [e,choice] of engraving.entries()){
    const id=460000+index*100+(++serial),selected=[{name:'Metal Choice',value:metal},{name:'Chain Length',value:length},{name:'Engraving',value:choice}];
    variants.push({id:'gid://shopify/ProductVariant/'+id,numericId:String(id),title:selected.map(o=>o.value).join(' / '),price:[49,59,199][m]+l*5+e*10,available:true,options:selected});
  }
  const productHandle=index===0?handle:'native-client46-'+index;
  return {id:'gid://shopify/Product/'+(4600+index),handle:productHandle,title:index===0?title:'Client46 Necklace '+index,type:'Necklace',url:'https://britesjewelry.com/products/'+productHandle,currency:'USD',description:'Published metal, chain and engraving choices.',image:'https://cdn.shopify.com/'+productHandle+'.jpg',storeCategories:[categories[index%5]],options,variants,variantsComplete:true,detailState:'checked'};
}
async function fixture(t,{provider=false}={}){
  const errors=[],vc=new VirtualConsole();vc.on('jsdomError',error=>errors.push(error));
  const dom=new JSDOM(sources['concierge-sandbox.html'],{url:'https://preview.example/concierge-sandbox.html',runScripts:'outside-only',pretendToBeVisual:true,virtualConsole:vc});
  const w=dom.window,d=w.document,products=Array.from({length:120},(_,i)=>product(i)),sent=[],requests=[],controls=[],calls=[],transcripts=[],finalized=[];
  let channel,client,config,turn=0,responseSerial=0,callSerial=0,hidden=false;
  Object.defineProperty(d,'hidden',{get:()=>hidden});w.matchMedia=()=>({matches:false,addEventListener(){}});w.HTMLElement.prototype.scrollIntoView=function(){};w.scrollTo=()=>{};
  w.HTMLMediaElement.prototype.play=async function(){};w.HTMLMediaElement.prototype.pause=function(){};
  const fetch=async(raw,init={})=>{
    const url=new URL(raw,w.location.href);requests.push({url,body:init.body?JSON.parse(init.body):null});let value;
    if(url.pathname==='/api/growth/catalogue')value={live:true,checkedAt:Date.now(),products:products.map(p=>({...p,detailState:'unconfirmed',variants:p.variants.map(v=>({...v,available:false,availabilityKnown:false}))})),pageInfo:{hasNextPage:false,endCursor:null},seed:{schema:1,target:120,minimum:100,loaded:120,complete:true,partial:false,sourcePages:1,categoryCounts:Object.fromEntries(categories.map(c=>[c,24])),unfilledCategories:[]}};
    else if(url.pathname==='/api/growth/inventory'){const offset=Number(url.searchParams.get('offset')||0),rows=products.slice(offset,offset+24);value={live:true,checkedAt:Date.now(),products:rows,inventory:{schema:1,total:120,offset,loaded:rows.length},pageInfo:{hasNextPage:offset+24<120,nextOffset:offset+24<120?offset+24:null}};}
    else if(url.pathname==='/api/growth/product')value={live:true,checkedAt:Date.now(),product:products.find(p=>p.handle===url.searchParams.get('handle'))};
    else if(url.pathname==='/api/concierge-voice'){const action=JSON.parse(init.body).action;value=action==='capabilities'?{enabled:true,nativeAudio:true,publicDemo:true}:action==='start'?{sdp:SDP,stopToken:'synthetic-client46-only',maxDurationMs:120000}:{stopped:true};}
    else if(url.pathname==='/api/growth/events')value={ok:true};
    else if(url.pathname==='/api/concierge')value={live:true,reply:'Ordinary catalogue answer.',products:[],meanings:[],preferences:{},preserveSelection:true};
    else if(url.pathname==='/api/growth/storefront-services')value={schema:1,guidance:{},conflicts:[],offers:{items:[],status:'published_not_checkout_validated'}};
    else throw Error('Unexpected simulated route '+url.pathname);
    return {ok:true,status:200,json:async()=>clone(value)};
  };w.fetch=fetch;
  class Peer{
    constructor(){this.iceGatheringState='complete';this.connectionState='connected';}addTrack(){}close(){this.connectionState='closed';}
    createDataChannel(){channel={readyState:'connecting',send:value=>sent.push(JSON.parse(value)),close(){}};return channel;}
    async createOffer(){return {type:'offer',sdp:SDP};}async setLocalDescription(value){this.localDescription=value;}
    async setRemoteDescription(){channel.readyState='open';channel.onopen();}
  }
  const runtime={document:d,location:w.location,navigator:{mediaDevices:{getUserMedia:async()=>{const track={readyState:'live',stop(){this.readyState='ended';},addEventListener(){},removeEventListener(){}};return {getTracks:()=>[track],getAudioTracks:()=>[track]};}}},RTCPeerConnection:Peer,AbortController:w.AbortController,fetch,setTimeout:w.setTimeout.bind(w),clearTimeout:w.clearTimeout.bind(w),addEventListener:w.addEventListener.bind(w),removeEventListener:w.removeEventListener.bind(w)};
  w.BritesConciergeVoice={...Voice,create(options){config=options;client=Voice.create({...options,runtime,greeting:false,
    onTranscript:value=>{transcripts.push(clone(value));options.onTranscript(value);},
    onFinalizedTurn:async value=>{finalized.push({value:{...value},result:null});const record=finalized.at(-1);record.result=provider?{handled:false}:await options.onFinalizedTurn(value);return record.result;},
    onTool:async(args,context)=>{const record={args:clone(args),context:{...context}};calls.push(record);record.result=await options.onTool(args,context);return record.result;}
  });return client;}};
  w.BritesConciergeAvatar={create(){return {setState(){},setEmotion(){},setVisible(){},setPaused(){},triggerGreeting(){},clearFocus(){},focusProduct(){},setLevel(){},setSpeechSignal(){},clearProduct(){},showProduct(){},cancelPerformance(){},setFloating(){},cue(){}};}};
  for(const name of ['brites-catalogue-intents.js','brites-concierge-shopping-guide.js','brites-storefront-bridge.js','concierge-sandbox.js'])w.eval(sources[name]);
  await settle();const store=w.BritesSandboxStorefront;await store.preloadInventory();const execute=store.execute;
  w.BritesSandboxStorefront={...store,async execute(action,options){controls.push({action:clone(action),requestId:options?.requestId,reviewAuthority:!!options?.reviewAuthority});return execute(action,options);}};
  const script=d.createElement('script');script.src='/brites-concierge.js';script.dataset.sandbox='true';Object.defineProperty(d,'currentScript',{get:()=>script});w.eval(sources['brites-concierge.js']);
  const root=d.querySelector('brites-concierge').shadowRoot;w.BritesConcierge.open({focus:false});[...root.querySelectorAll('button')].find(b=>b.textContent==='Talk to me').click();await settle();root.querySelector('script[src$="brites-concierge-voice.js"]')?.dispatchEvent(new w.Event('load'));await settle();assert.equal(client.state,'listening');
  const emit=value=>channel.onmessage({data:JSON.stringify(value)}),responses=()=>sent.filter(e=>e.type==='response.create');
  function begin(){const itemId='native46-input-'+(++turn);emit({type:'input_audio_buffer.speech_started',item_id:itemId});return {itemId,version:turn+1};}
  const stopped=input=>emit({type:'input_audio_buffer.speech_stopped',item_id:input.itemId}),commit=input=>emit({type:'input_audio_buffer.committed',item_id:input.itemId});
  const final=(input,text)=>emit({type:'conversation.item.input_audio_transcription.completed',item_id:input.itemId,content_index:0,transcript:text});
  async function say(text){const input=begin();stopped(input);commit(input);final(input,text);await settle();return input;}
  function bind(request=responses().at(-1)){assert.ok(request);const responseId='native46-response-'+(++responseSerial);emit({type:'response.created',response:{id:responseId,status:'in_progress',metadata:request.response.metadata}});return responseId;}
  const done=(responseId,output=[],status='completed')=>emit({type:'response.done',response:{id:responseId,status,output}});
  function functionItem(name,args){const id='native46-function-'+(++callSerial);return {id,type:'function_call',status:'completed',name,call_id:'native46-call-'+callSerial,arguments:JSON.stringify(args)};}
  const added=(responseId,item)=>emit({type:'response.output_item.added',response_id:responseId,output_index:0,item:{...item,status:'in_progress',arguments:''}});
  const argumentsDone=(responseId,item,extra={})=>emit({type:'response.function_call_arguments.done',response_id:responseId,call_id:item.call_id,item_id:item.id,output_index:0,arguments:item.arguments,...extra});
  const itemDone=(responseId,item)=>emit({type:'response.output_item.done',response_id:responseId,output_index:0,item});
  const outputs=callId=>sent.filter(e=>e.item?.type==='function_call_output'&&e.item.call_id===callId);
  async function ready(){await store.execute({type:'open',handle});for(const [optionName,optionValue] of [['Metal Choice','14k Gold Filled'],['Chain Length','16 inch'],['Engraving','None']])assert.equal((await store.execute({type:'select-option',handle,optionName,optionValue})).ok,true);await settle();controls.length=0;}
  function finishSpeech(){const id=bind();emit({type:'output_audio_buffer.started',response_id:id});done(id);emit({type:'output_audio_buffer.stopped',response_id:id});}
  const cart=()=>JSON.parse(w.sessionStorage.getItem('brites-sandbox-cart')||'[]');
  t.after(async()=>{w.BritesConcierge.close();await client.dispose();w.close();assert.deepEqual(errors,[]);});
  return {w,d,root,store,products,sent,requests,controls,calls,transcripts,finalized,emit,responses,begin,stopped,commit,final,say,bind,done,functionItem,added,argumentsDone,itemDone,outputs,ready,finishSpeech,cart,get client(){return client;},get config(){return config;},hide(){hidden=true;d.dispatchEvent(new w.Event('visibilitychange'));}};
}
function safeCustomerReply(value){assert.equal(typeof value.reply,'string');assert.ok(value.reply.trim());assert.doesNotMatch(value.reply,/backend|postcondition|authority|tool[_ -]?call|request[_ -]?id|completedActions|no further action/i);}
function controlReplyPacket(value){safeCustomerReply(value);assert.deepEqual(Object.keys(value),['reply']);}
async function providerControl(f,text,args,completion='stream'){
  const previous=f.responses().length,input=await f.say(text),responseId=f.bind(),item=f.functionItem('control_storefront',args);
  if(completion==='stream'){f.added(responseId,item);f.argumentsDone(responseId,item);f.itemDone(responseId,item);await settle();assert.equal(f.responses().length,previous+1,'the owning native generation must finish before speaking the result');}
  else if(completion==='item'){f.itemDone(responseId,item);await settle();assert.equal(f.responses().length,previous+1);}
  f.done(responseId,[item]);await settle();assert.equal(f.outputs(item.call_id).length,1);assert.equal(f.responses().length,previous+2);assert.equal(f.responses().at(-1).response.tool_choice,'none');
  const receipt=JSON.parse(f.outputs(item.call_id)[0].item.output),host=f.calls.find(c=>c.context.callId===item.call_id)?.result;controlReplyPacket(receipt);assert.equal(receipt.reply,Voice.shopperReply(host));f.finishSpeech();return {input,item,responseId,receipt,host};
}

test('actual native function item supplies a missing arguments-event name and adds the exact ready 59 USD necklace once',async t=>{
  const f=await fixture(t,{provider:true});await f.ready();const input=await f.say('Add this piece to my cart'),responseId=f.bind(),item=f.functionItem('control_storefront',{type:'add',handle});
  f.added(responseId,item);f.argumentsDone(responseId,item);await settle();
  assert.equal(f.calls.length,1,'the native function name must be correlated from the output item');assert.equal(f.calls[0].context.name,'control_storefront');assert.equal(f.calls[0].context.inputItemId,input.itemId);assert.equal(f.calls[0].context.turnVersion,input.version);assert.equal(f.calls[0].context.currentTurn,true);
  assert.equal(f.cart().length,1);assert.equal(f.cart()[0].variant,'14k Gold Filled / 16 inch / None');assert.equal(f.cart()[0].price,59);assert.equal(f.cart()[0].quantity||1,1);assert.equal(f.d.querySelector('#bag-count').textContent,'1');assert.equal(f.responses().length,1,'speech cannot overlap the owning generation');
  f.itemDone(responseId,item);f.done(responseId,[item]);await settle();assert.equal(f.calls.length,1);assert.equal(f.controls.filter(c=>c.action.type==='add').length,1);assert.equal(f.outputs(item.call_id).length,1);assert.equal(f.calls[0].result.ok,true);assert.equal(f.calls[0].result.cartChanged,true);controlReplyPacket(JSON.parse(f.outputs(item.call_id)[0].item.output));assert.equal(f.responses().length,2);assert.equal(f.responses()[1].response.tool_choice,'none');
});

test('documented complete response.done function output routes an exact native control without requiring streamed argument events',async t=>{
  const f=await fixture(t,{provider:true});await f.ready();await f.say('Set quantity to two');const responseId=f.bind(),item=f.functionItem('control_storefront',{type:'product-quantity',handle,quantity:2});
  f.done(responseId,[item]);await settle();assert.equal(f.calls.length,1);assert.equal(f.store.snapshot().productControls.quantity,2);assert.equal(f.d.querySelector('input[aria-label="Quantity of this exact piece"]').value,'2');assert.equal(f.outputs(item.call_id).length,1);assert.equal(f.responses().length,2);assert.equal(f.responses()[1].response.tool_choice,'none');assert.deepEqual(f.cart(),[]);
});

test('native finalized ASR changes every screenshot choice and quantity, then adds and opens the actual cart without a provider action replay',async t=>{
  const f=await fixture(t);await f.store.execute({type:'open',handle});await settle();
  for(const text of ['Open the metal menu','Select 14k Gold Filled','Select 16 inch','Select None','Set quantity to two','Add this piece to my cart','Take me to my cart']){
    const before=f.responses().length,input=await f.say(text),result=f.finalized.at(-1);
    assert.equal(result.value.inputItemId,input.itemId);assert.equal(result.value.turnVersion,input.version);assert.equal(result.value.currentTurn,true);assert.equal(result.result.handled,true,text);assert.equal(result.result.ok,true,JSON.stringify({text,result:result.result}));safeCustomerReply(result.result);
    assert.equal(f.responses().length,before+1);assert.equal(f.responses().at(-1).response.tool_choice,'none');f.finishSpeech();assert.equal(f.client.state,'listening');
  }
  assert.equal(f.calls.length,0);assert.equal(f.store.snapshot().pageKind,'bag');assert.equal(f.cart().length,1);assert.equal(f.cart()[0].variant,'14k Gold Filled / 16 inch / None');assert.equal(f.cart()[0].price,59);assert.equal(f.cart()[0].quantity,2);assert.equal(f.d.querySelector('[data-bag-line] input[type="number"]').value,'2');assert.equal(f.d.querySelector('#bag-count').textContent,'2');assert.equal(f.controls.filter(c=>c.action.type==='add').length,1);assert.ok(f.controls.every(c=>/^voice-/.test(c.requestId)));assert.equal(f.controls.find(c=>c.action.type==='add').reviewAuthority,true);
  assert.equal(f.requests.filter(r=>r.url.pathname==='/api/concierge'&&r.body?.message).length,0,'native page controls cannot detour into the typed conversation API');
});

test('final transcript buffered before commit is delivered to the real host once with its exact native item and version',async t=>{
  const f=await fixture(t);await f.ready();const input=f.begin();f.stopped(input);f.final(input,'Add this piece to my cart');await settle();assert.equal(f.finalized.length,0);assert.deepEqual(f.cart(),[]);assert.equal(f.responses().length,0);
  f.commit(input);await settle();assert.equal(f.finalized.length,1);assert.equal(f.cart().length,1);assert.equal(f.responses().length,1);
  const final=f.transcripts.filter(v=>v.role==='user'&&v.final);assert.deepEqual(final,[{role:'user',text:'Add this piece to my cart',final:true,itemId:input.itemId,turnVersion:input.version,currentTurn:true}]);
  f.final(input,'Add another necklace to my cart');f.commit(input);await settle();assert.equal(f.finalized.length,1);assert.equal(f.cart()[0].quantity||1,1);assert.equal(f.controls.filter(c=>c.action.type==='add').length,1);assert.equal(f.responses().length,1);
});

test('partial native ASR cannot change the ready choices, add a cart line or start a response',async t=>{
  const f=await fixture(t);await f.ready();const before=clone(f.store.snapshot().productControls),input=f.begin();f.stopped(input);f.commit(input);f.emit({type:'conversation.item.input_audio_transcription.delta',item_id:input.itemId,content_index:0,delta:'Add this piece to my cart'});await settle();
  assert.equal(f.finalized.length,0);assert.equal(f.calls.length,0);assert.equal(f.controls.length,0);assert.deepEqual(f.cart(),[]);assert.deepEqual(clone(f.store.snapshot().productControls.selectedOptions),before.selectedOptions);assert.equal(f.responses().length,0);
});

test('late old native ASR cannot adopt the newer product quantity turn or replay its original add request',async t=>{
  const f=await fixture(t);await f.ready();const old=f.begin();f.stopped(old);f.commit(old);const fresh=await f.say('Set quantity to two');f.final(old,'Add this piece to my cart');f.commit(old);await settle();
  assert.equal(f.finalized.length,1);assert.equal(f.finalized[0].value.inputItemId,fresh.itemId);assert.equal(f.store.snapshot().productControls.quantity,2);assert.deepEqual(f.cart(),[]);assert.equal(f.controls.filter(c=>c.action.type==='add').length,0);assert.equal(f.responses().length,1);
});

test('real provider completion shapes control options, quantity, exact add and stable cart edits under finalized native authority',async t=>{
  const f=await fixture(t,{provider:true});await f.ready();
  let out=await providerControl(f,'Open the metal menu',{type:'options',handle,optionName:'Metal Choice'});assert.equal(out.host.ok,true);assert.equal(f.store.snapshot().productControls.openedOption,'Metal Choice');
  out=await providerControl(f,'Select 14k Solid Gold',{type:'select-option',handle,optionName:'Metal Choice',optionValue:'14k Solid Gold'},'item');assert.equal(out.host.ok,true);assert.equal(f.d.querySelector('[data-option-name="Metal Choice"] [data-option-value="14k Solid Gold"]').getAttribute('aria-pressed'),'true');
  out=await providerControl(f,'Set quantity to two',{type:'product-quantity',handle,quantity:2},'terminal');assert.equal(out.host.ok,true);assert.equal(f.d.querySelector('input[aria-label="Quantity of this exact piece"]').value,'2');
  out=await providerControl(f,'Add this piece to my cart',{type:'add',handle});assert.equal(out.host.ok,true);assert.equal(f.cart().length,1);assert.equal(f.cart()[0].variant,'14k Solid Gold / 16 inch / None');assert.equal(f.cart()[0].quantity,2);
  out=await providerControl(f,'Take me to my cart',{type:'bag'},'terminal');assert.equal(out.host.ok,true);const line=f.store.snapshot().bagControls.lines[0];assert.ok(line.lineId);assert.equal(f.d.querySelector('[data-bag-line] input[type="number"]').value,'2');
  out=await providerControl(f,'Set the quantity of the first item in my cart to three',{type:'bag-quantity',lineId:line.lineId,quantity:3},'item');assert.equal(out.host.ok,true);assert.equal(f.cart()[0].quantity,3);assert.equal(f.d.querySelector('[data-bag-line] input[type="number"]').value,'3');assert.equal(f.store.snapshot().bagControls.lines[0].lineId,line.lineId);
  out=await providerControl(f,'Remove the first item from my cart',{type:'bag-remove',lineId:line.lineId});assert.equal(out.host.ok,true);assert.deepEqual(f.cart(),[]);assert.equal(f.d.querySelector('[data-bag-line]'),null);assert.equal(f.calls.length,7);assert.equal(f.controls.filter(c=>c.action.type==='add').length,1);assert.ok(f.controls.every(c=>/^voice-/.test(c.requestId)));assert.equal(f.controls.find(c=>c.action.type==='add').reviewAuthority,true);
});

test('an argument event without an item identity never consumes a later valid completed item as a failed catalogue read',async t=>{
  const f=await fixture(t,{provider:true});await f.ready();await f.say('Add this piece to my cart');const id=f.bind(),item=f.functionItem('control_storefront',{type:'add',handle});
  f.argumentsDone(id,item);await settle();assert.equal(f.calls.length,0);assert.equal(f.outputs(item.call_id).length,0);assert.deepEqual(f.cart(),[]);f.itemDone(id,item);f.done(id,[item]);await settle();assert.equal(f.calls.length,1);assert.equal(f.outputs(item.call_id).length,1);assert.equal(f.cart().length,1);assert.equal(f.responses().length,2);
});

test('all duplicated native function completion forms and terminal events still execute an authorized cart add once',async t=>{
  const f=await fixture(t,{provider:true});await f.ready();await f.say('Add this piece to my cart');const id=f.bind(),item=f.functionItem('control_storefront',{type:'add',handle});f.added(id,item);
  for(let n=0;n<3;n++){f.argumentsDone(id,item);f.itemDone(id,item);}f.done(id,[item,item]);f.done(id,[item]);await settle();
  assert.equal(f.calls.length,1);assert.equal(f.outputs(item.call_id).length,1);assert.equal(f.cart().length,1);assert.equal(f.cart()[0].quantity||1,1);assert.equal(f.controls.filter(c=>c.action.type==='add').length,1);assert.equal(f.responses().length,2);
});

test('foreign response metadata cannot authorize an apparently complete native cart function item',async t=>{
  const f=await fixture(t,{provider:true});await f.ready();await f.say('Add this piece to my cart');const request=f.responses().at(-1),id='foreign-response-46',item=f.functionItem('control_storefront',{type:'add',handle});
  f.emit({type:'response.created',response:{id,metadata:{...request.response.metadata,brites_input_item:'not-the-native-item'}}});f.added(id,item);f.argumentsDone(id,item,{name:'control_storefront'});f.itemDone(id,item);f.done(id,[item]);await settle();
  assert.equal(f.calls.length,0);assert.equal(f.controls.length,0);assert.deepEqual(f.cart(),[]);assert.equal(f.responses().length,1);
});

test('conflicting call and function identities cannot borrow the known output item to authorize a cart add',async t=>{
  const f=await fixture(t,{provider:true});await f.ready();await f.say('Add this piece to my cart');const id=f.bind(),item=f.functionItem('control_storefront',{type:'add',handle});f.added(id,item);
  f.argumentsDone(id,item,{call_id:'foreign-call-46'});f.argumentsDone(id,item,{name:'inspect_jewellery'});await settle();assert.equal(f.calls.length,0);assert.deepEqual(f.cart(),[]);
  f.itemDone(id,{...item,name:'inspect_jewellery',arguments:JSON.stringify({handle})});f.done(id,[item]);await settle();assert.equal(f.calls.length,0);assert.equal(f.controls.length,0);assert.deepEqual(f.cart(),[]);
});

test('stale complete output items cannot perform a cart action after genuine newer native words',async t=>{
  const f=await fixture(t,{provider:true});await f.ready();await f.say('Add this piece to my cart');const old=f.bind(),item=f.functionItem('control_storefront',{type:'add',handle});f.added(old,item);await f.say('Tell me about this necklace');
  f.argumentsDone(old,item);f.itemDone(old,item);f.done(old,[item]);await settle();assert.equal(f.calls.length,0);assert.equal(f.controls.length,0);assert.deepEqual(f.cart(),[]);assert.equal(f.responses().length,2);
});

for(const status of ['cancelled','failed','incomplete'])test('a '+status+' response cannot execute a terminal-only completed-looking cart function',async t=>{
  const f=await fixture(t,{provider:true});await f.ready();await f.say('Add this piece to my cart');const id=f.bind(),item=f.functionItem('control_storefront',{type:'add',handle});f.done(id,[item],status);await settle();
  assert.equal(f.calls.length,0);assert.equal(f.controls.length,0);assert.deepEqual(f.cart(),[]);assert.equal(f.outputs(item.call_id).length,0);assert.equal(f.responses().length,1);
});

test('a completed item from a tool-free finalized reply cannot execute another cart operation',async t=>{
  const f=await fixture(t);await f.ready();await f.say('Add this piece to my cart');assert.equal(f.cart().length,1);assert.equal(f.responses().at(-1).response.tool_choice,'none');const id=f.bind(),item=f.functionItem('control_storefront',{type:'add',handle});f.added(id,item);f.argumentsDone(id,item,{name:'control_storefront'});f.itemDone(id,item);f.done(id,[item]);await settle();
  assert.equal(f.calls.length,0);assert.equal(f.cart().length,1);assert.equal(f.cart()[0].quantity||1,1);assert.equal(f.controls.filter(c=>c.action.type==='add').length,1);assert.equal(f.responses().length,1);
});

test('an incomplete output item waits for successful response completion instead of asserting fresh action authority',async t=>{
  const f=await fixture(t,{provider:true});await f.ready();await f.say('Set quantity to two');const id=f.bind(),item=f.functionItem('control_storefront',{type:'product-quantity',handle,quantity:2});f.itemDone(id,{...item,status:'incomplete'});await settle();
  assert.equal(f.calls.length,0);assert.equal(f.store.snapshot().productControls.quantity,1);f.done(id,[item]);await settle();assert.equal(f.calls.length,1);assert.equal(f.store.snapshot().productControls.quantity,2);assert.equal(f.outputs(item.call_id).length,1);
});

test('a native presentation function correlated from its output item never enters the shopping callback or consumes cart authority',async t=>{
  const f=await fixture(t,{provider:true});await f.ready();await f.say('Tell me about this necklace');const id=f.bind(),item=f.functionItem('set_avatar_performance',{mood:'warm',gesture:'present',intensity:.4,durationMs:800});
  f.added(id,item);f.argumentsDone(id,item);f.itemDone(id,item);f.done(id,[item]);await settle();assert.equal(f.calls.length,0);assert.equal(f.controls.length,0);assert.deepEqual(f.cart(),[]);assert.equal(f.outputs(item.call_id).length,1);assert.deepEqual(JSON.parse(f.outputs(item.call_id)[0].item.output),{accepted:true,presentationOnly:true});assert.equal(f.responses().length,2);assert.equal(f.responses().at(-1).response.tool_choice,'none');
});

test('strict native argument validation applies equally to complete function items and produces only a safe failure',async t=>{
  const f=await fixture(t,{provider:true});await f.ready();await f.say('Add this piece to my cart');const id=f.bind(),item=f.functionItem('control_storefront',{type:'add',handle,quantity:8});
  f.itemDone(id,item);f.done(id,[item]);await settle();assert.equal(f.calls.length,0);assert.equal(f.controls.length,0);assert.deepEqual(f.cart(),[]);assert.equal(f.outputs(item.call_id).length,1);const result=JSON.parse(f.outputs(item.call_id)[0].item.output);controlReplyPacket(result);assert.equal(result.reply,'I couldn’t finish that change. Please check the visible options.');assert.equal(f.responses().at(-1).response.tool_choice,'none');
});

test('unrelated native shopper words cannot lend completed provider function items authority to add a cart line',async t=>{
  const f=await fixture(t,{provider:true});await f.ready();await f.say('Tell me what this necklace symbolizes');const id=f.bind(),item=f.functionItem('control_storefront',{type:'add',handle});
  f.itemDone(id,item);f.done(id,[item]);await settle();assert.equal(f.calls.length,1,'a protocol-valid call still reaches the real shopper authority guard');assert.notEqual(f.calls[0].result.ok,true);assert.equal(f.controls.length,0);assert.deepEqual(f.cart(),[]);assert.equal(f.outputs(item.call_id).length,1);controlReplyPacket(JSON.parse(f.outputs(item.call_id)[0].item.output));assert.equal(f.responses().at(-1).response.tool_choice,'none');
});
