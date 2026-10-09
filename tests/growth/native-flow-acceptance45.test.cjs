'use strict';
// Independent integrated checks of the real native client, widget, bridge and
// sandbox host. ASR/provider events, media and HTTP are synthetic. These checks
// certify native event/tool routing and visible controls, not physical audio.
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs');
const {JSDOM,VirtualConsole}=require('jsdom');
const Voice=require('../../brites-concierge-voice.js');
const files=['concierge-sandbox.html','brites-catalogue-intents.js','brites-concierge-shopping-guide.js','brites-storefront-bridge.js','concierge-sandbox.js','brites-concierge.js'];
const sources=Object.fromEntries(files.map(name=>[name,fs.readFileSync(require.resolve('../../'+name),'utf8')]));
const SDP='v=0\r\nm=audio 9 UDP/TLS/RTP/SAVPF 111\r\na=candidate:1 1 UDP 2122260223 192.0.2.10 50000 typ host\r\n';
const clone=value=>JSON.parse(JSON.stringify(value));
const settle=async()=>{for(let i=0;i<10;i++)await new Promise(setImmediate);};
const until=async(predicate,diagnostic)=>{const deadline=Date.now()+3000;while(!predicate()&&Date.now()<deadline)await new Promise(resolve=>setTimeout(resolve,15));assert.ok(predicate(),diagnostic?diagnostic():'expected voice state transition did not finish');};
const handle='native-flow-0',title='Otter Blossom Necklace';
const categories=['regular-necklaces','beady-necklaces','stud-earrings','hoop-earrings','charm-only'];
function product(index,{engraving=false,twoGoldChoices=false}={}){
  const metal=['Sterling Silver','14k Gold Filled',...(twoGoldChoices?['10k Solid Gold']:[]),'14k Solid Gold'];
  const productHandle='native-flow-'+index,type=index%5===4?'Charm':index%5<2?'Necklace':'Earrings';
  const options=[{name:'Metal Choice',values:metal},{name:'Chain Length',values:['16 inches','18 inches']},...(engraving?[{name:'Engraving',values:['None','Engraved']}]:[])];
  const variants=[];let serial=0;
  for(const [m,material] of metal.entries())for(const [l,length] of ['16 inches','18 inches'].entries())for(const engraved of engraving?['None','Engraved']:['None']){
    const id=45000+index*100+(++serial),choices=[{name:'Metal Choice',value:material},{name:'Chain Length',value:length},...(engraving?[{name:'Engraving',value:engraved}]:[])];
    variants.push({id:'gid://shopify/ProductVariant/'+id,numericId:String(id),title:choices.map(o=>o.value).join(' / '),price:50+m*100+l*5+(engraved==='Engraved'?10:0),available:true,options:choices});
  }
  return {id:'gid://shopify/Product/'+(4500+index),handle:productHandle,title:index===0?title:'Native Flow '+index+' '+type,type,url:'https://britesjewelry.com/products/'+productHandle,currency:'USD',description:'Published otter jewellery with selectable chain lengths and literal metal choices.',image:'https://cdn.shopify.com/'+productHandle+'.jpg',storeCategories:[categories[index%5]],options,variants,variantsComplete:true,checkedAt:Date.now(),detailState:'checked'};
}
async function fixture(t,{providerTools=false,engraving=false,twoGoldChoices=false,serverStopConfirmed=true,renewalRateLimits=0}={}){
  const errors=[],console=new VirtualConsole();console.on('jsdomError',error=>errors.push(error));
  const dom=new JSDOM(sources['concierge-sandbox.html'],{url:'https://preview.example/concierge-sandbox.html',runScripts:'outside-only',pretendToBeVisual:true,virtualConsole:console});
  const w=dom.window,d=w.document,products=Array.from({length:120},(_,i)=>product(i,{engraving:i===0&&engraving,twoGoldChoices:i===0&&twoGoldChoices}));
  const requests=[],controls=[],sent=[],calls=[],finalizedResults=[];let channel,client,config,turn=0,responseSerial=0,callSerial=0,hidden=false,voiceStarts=0;
  Object.defineProperty(d,'hidden',{get:()=>hidden});
  d.body.dataset.inventoryPreload='complete';w.matchMedia=()=>({matches:false,addEventListener(){}});w.HTMLElement.prototype.scrollIntoView=function(){};w.scrollTo=()=>{};
  w.HTMLMediaElement.prototype.play=async function(){};w.HTMLMediaElement.prototype.pause=function(){};
  const fetch=async(raw,init={})=>{
    const url=new URL(raw,w.location.href);requests.push({url,init});let value;
    if(url.pathname==='/api/growth/catalogue')value={live:true,checkedAt:Date.now(),products:products.map(p=>({...p,detailState:'unconfirmed',variants:p.variants.map(v=>({...v,available:false,availabilityKnown:false}))})),pageInfo:{hasNextPage:false,endCursor:null},seed:{schema:1,target:120,minimum:100,loaded:120,complete:true,partial:false,sourcePages:1,categoryCounts:Object.fromEntries(categories.map(c=>[c,24])),unfilledCategories:[]}};
    else if(url.pathname==='/api/growth/inventory'){
      const offset=Number(url.searchParams.get('offset')),rows=products.slice(offset,offset+24);value={live:true,checkedAt:Date.now(),products:rows,inventory:{schema:1,total:120,offset,limit:24,loaded:rows.length,detailsLoaded:rows.length,ready:false,partial:false,expiresAt:Date.now()+300000},pageInfo:{hasNextPage:offset+24<120,nextOffset:offset+24<120?offset+24:null}};
    }else if(url.pathname==='/api/growth/product')value={live:true,checkedAt:Date.now(),product:products.find(p=>p.handle===url.searchParams.get('handle'))};
    else if(url.pathname==='/api/concierge-voice'){
      const action=JSON.parse(init.body).action;
      if(action==='start'&&++voiceStarts>1&&voiceStarts<=1+renewalRateLimits)return {ok:false,status:429,json:async()=>({code:'VOICE_RATE_LIMITED',error:'Please wait before starting another voice session.',retryAfterMs:1})};
      value=action==='capabilities'?{enabled:true,nativeAudio:true,publicDemo:true}:action==='start'?{sdp:SDP,stopToken:'native-flow-synthetic-only',maxDurationMs:120000}:{stopped:serverStopConfirmed};
    }else if(url.pathname==='/api/growth/events')value={ok:true};
    else if(url.pathname==='/api/concierge')value={live:true,reply:'Unexpected ordinary catalogue detour.',products:[],meanings:[],preferences:{},preserveSelection:true};
    else throw Error('Unexpected fixture route '+url.pathname);
    return {ok:true,status:200,json:async()=>clone(value)};
  };w.fetch=fetch;
  class Peer{
    constructor(){this.iceGatheringState='complete';}addTrack(){}close(){}
    createDataChannel(){channel={readyState:'connecting',send:value=>sent.push(JSON.parse(value)),close(){}};return channel;}
    async createOffer(){return {type:'offer',sdp:SDP};}async setLocalDescription(value){this.localDescription=value;}
    async setRemoteDescription(){channel.readyState='open';channel.onopen();}
  }
  const runtime={document:d,location:w.location,navigator:{mediaDevices:{getUserMedia:async()=>{const track={readyState:'live',stop(){this.readyState='ended';},addEventListener(){},removeEventListener(){}};return {getTracks:()=>[track],getAudioTracks:()=>[track]};}}},RTCPeerConnection:Peer,AbortController:w.AbortController,fetch,setTimeout:w.setTimeout.bind(w),clearTimeout:w.clearTimeout.bind(w),addEventListener:w.addEventListener.bind(w),removeEventListener:w.removeEventListener.bind(w)};
  w.BritesConciergeVoice={publicContext:Voice.publicContext,create(options){
    config=options;
    client=Voice.create({...options,runtime,greeting:false,
      // Keep real committed transcription, authority and transport. Disable only
      // the optional local fast hook so the provider callback is exercised too.
      onFinalizedTurn:providerTools?async()=>({handled:false}):async value=>{const result=await options.onFinalizedTurn(value);finalizedResults.push(result);return result;},
      onTool:async(args,context)=>{const record={args:clone(args),context};calls.push(record);record.result=await options.onTool(args,context);return record.result;}
    });return client;
  }};
  w.BritesConciergeAvatar={create(){return {setState(){},setEmotion(){},setVisible(){},setPaused(){},triggerGreeting(){},clearFocus(){},focusProduct(){},setLevel(){},setSpeechSignal(){},clearProduct(){},showProduct(){},cancelPerformance(){},setFloating(){},cue(){}};}};
  for(const name of ['brites-catalogue-intents.js','brites-concierge-shopping-guide.js','brites-storefront-bridge.js','concierge-sandbox.js'])w.eval(sources[name]);
  await settle();const store=w.BritesSandboxStorefront;await store.preloadInventory();
  const execute=store.execute;w.BritesSandboxStorefront={...store,async execute(action,options){controls.push(clone(action));return execute(action,options);}};
  const script=d.createElement('script');script.src='/brites-concierge.js';script.dataset.sandbox='true';Object.defineProperty(d,'currentScript',{get:()=>script});w.eval(sources['brites-concierge.js']);
  const root=d.querySelector('brites-concierge').shadowRoot;w.BritesConcierge.open({focus:false});[...root.querySelectorAll('button')].find(b=>b.textContent==='Talk to me').click();await settle();root.querySelector('script[src$="brites-concierge-voice.js"]')?.dispatchEvent(new w.Event('load'));await settle();assert.equal(client.state,'listening');
  const emit=value=>channel.onmessage({data:JSON.stringify(value)}),responses=()=>sent.filter(e=>e.type==='response.create');
  const receipts=()=>sent.filter(e=>/^(?:Host-completed result|Customer reply for this finalized shopper request)/.test(e.item?.content?.[0]?.text||''));
  function begin(){const itemId='native45-input-'+(++turn);emit({type:'input_audio_buffer.speech_started',item_id:itemId});emit({type:'input_audio_buffer.speech_stopped',item_id:itemId});return {itemId,version:turn+1};}
  const commit=input=>emit({type:'input_audio_buffer.committed',item_id:input.itemId});
  const final=(input,text)=>emit({type:'conversation.item.input_audio_transcription.completed',item_id:input.itemId,transcript:text});
  async function say(text){const input=begin();commit(input);final(input,text);await settle();return input;}
  function result(){return finalizedResults.at(-1)||null;}
  async function provider(name,args){
    const issued=responses().at(-1);assert.ok(issued,'a finalized current response must be issued');
    const responseId='native45-response-'+(++responseSerial),callId='native45-call-'+(++callSerial);emit({type:'response.created',response:{id:responseId,metadata:issued.response.metadata}});
    emit({type:'response.function_call_arguments.done',response_id:responseId,call_id:callId,name,arguments:JSON.stringify(args)});await settle();
    const output=sent.find(e=>e.item?.type==='function_call_output'&&e.item.call_id===callId);assert.ok(output,'the native tool must return an explicit result');
    const modelReceipt=JSON.parse(output.item.output),hostResult=calls.find(c=>c.context.callId===callId)?.result;
    const result={...(hostResult||modelReceipt)};Object.defineProperty(result,'modelReceipt',{value:modelReceipt});return result;
  }
  function selected(){return Object.fromEntries(store.snapshot().productControls.selectedOptions.map(o=>[o.name,o.value]));}
  function cart(){return JSON.parse(w.sessionStorage.getItem('brites-sandbox-cart')||'[]');}
  t.after(async()=>{w.BritesConcierge.close();await client.dispose();w.close();assert.deepEqual(errors,[]);});
  const catalogueCalls=()=>requests.filter(r=>r.url.pathname==='/api/concierge'&&typeof JSON.parse(r.init.body||'{}').message==='string');
  const startCount=()=>requests.filter(r=>r.url.pathname==='/api/concierge-voice'&&JSON.parse(r.init.body||'{}').action==='start').length;
  return {w,d,root,store,products,requests,catalogueCalls,startCount,controls,calls,sent,emit,responses,receipts,begin,commit,final,say,result,provider,selected,cart,get config(){return config;},get client(){return client;},hide(){hidden=true;d.dispatchEvent(new w.Event('visibilitychange'));}};
}
function customerText(result){return [result?.reply,result?.message,result?.error,result?.reason].filter(value=>typeof value==='string').join(' ');}
function assertShopperWording(result){
  const text=customerText(result?.modelReceipt||result);assert.ok(text,'a completed or clarified request needs customer wording');
  assert.doesNotMatch(text,/motif|backend|postcondition|authority|adapter|request[_ -]?id|turn[_ -]?version|transcript|control[_ -]?version|no (?:further|extra) (?:action|website action)|nothing was changed|finalized request|completedActions/i);
}
async function openCurrent(f){assert.equal((await f.store.execute({type:'open',handle})).ok,true);await settle();}
async function exactSelection(f,{engraving=false}={}){
  for(const [optionName,optionValue] of [['Metal Choice','14k Solid Gold'],['Chain Length','16 inches'],...(engraving?[['Engraving','None']]:[])])assert.equal((await f.store.execute({type:'select-option',handle,optionName,optionValue})).ok,true);
  await settle();
}

test('finalized native home request opens the genuine homepage from a detailed listing',async t=>{
  const f=await fixture(t);await openCurrent(f);await f.say('Take me to the homepage');
  const page=f.store.snapshot();assert.equal(page.currentHandle,'');assert.ok(['home','collection','catalogue'].includes(page.pageKind));
  assert.ok(f.d.querySelector('.collection-intro'),'the actual homepage intro must be restored');assert.equal(f.d.querySelector('.product-layout'),null);assert.equal(f.result()?.ok,true);assertShopperWording(f.result());
  assert.equal(new URL(f.w.location.href).searchParams.has('product'),false,'home URL must no longer point at the previous product');
  assert.equal(f.catalogueCalls().length,0,'a home action must not send a conversational catalogue request');
});

test('finalized current 16-inch solid-gold necklace request changes both native option controls',async t=>{
  const f=await fixture(t);await openCurrent(f);await f.say('Select a 16 inch solid gold necklace for me');
  assert.equal(f.store.snapshot().currentHandle,handle);assert.deepEqual(f.selected(),{'Metal Choice':'14k Solid Gold','Chain Length':'16 inches'});
  const pc=f.store.snapshot().productControls,variant=f.products[0].variants.find(v=>v.title==='14k Solid Gold / 16 inches');assert.equal(pc.variantId,variant.id);assert.equal(f.d.querySelector('#piece-variant').value,variant.id);assert.equal(pc.quantity,1);
  assert.equal(f.d.querySelector('[data-option-name="Metal Choice"] [data-option-value="14k Solid Gold"]').getAttribute('aria-pressed'),'true');
  assert.equal(f.d.querySelector('[data-option-name="Chain Length"] [data-option-value="16 inches"]').getAttribute('aria-pressed'),'true');assert.equal(f.result()?.ok,true);assertShopperWording(f.result());
  assert.equal(f.cart().length,0);assert.equal(f.catalogueCalls().length,0);
});

test('ambiguous solid-gold construction asks the shopper for karat while preserving every unchosen native control',async t=>{
  const f=await fixture(t,{twoGoldChoices:true});await openCurrent(f);await f.say('Select a 16 inch solid gold necklace for me');
  const pc=f.store.snapshot().productControls;assert.equal(pc.variantId,null);assert.equal(pc.selectedOptions.some(o=>o.name==='Metal Choice'&&o.value),false);assert.equal(f.cart().length,0);
  assert.match(customerText(f.result()),/10k.*14k|14k.*10k/i);assertShopperWording(f.result());
});

test('native provider option tool uses finalized shopper choice when a valid equivalent variant argument differs',async t=>{
  const f=await fixture(t,{providerTools:true});await openCurrent(f);await f.store.execute({type:'select-option',handle,optionName:'Chain Length',optionValue:'16 inches'});await settle();
  const input=await f.say('Select solid gold'),variant=f.products[0].variants.find(v=>v.title==='14k Solid Gold / 16 inches');
  const out=await f.provider('control_storefront',{type:'select-option',handle,variantId:variant.id});
  assert.equal(out.ok,true,JSON.stringify(out));assert.equal(f.d.querySelector('#piece-variant').value,variant.id);assert.deepEqual(f.selected(),{'Chain Length':'16 inches','Metal Choice':'14k Solid Gold'});assertShopperWording(out);
  assert.equal(f.calls[0].context.inputItemId,input.itemId);assert.equal(f.controls.filter(a=>a.type==='select-option').length,1);
});

test('native provider discovery misroute still executes the actual current shopper compound option request',async t=>{
  const f=await fixture(t,{providerTools:true});await openCurrent(f);await f.say('Select a 16 inch solid gold necklace for me');
  const out=await f.provider('find_jewellery',{message:'Find a necklace with a gold motif'});
  assert.equal(out.ok,true,JSON.stringify(out));assert.deepEqual(f.selected(),{'Metal Choice':'14k Solid Gold','Chain Length':'16 inches'});assertShopperWording(out);
  assert.equal(f.catalogueCalls().length,0);assert.equal(f.cart().length,0);
});

test('native provider home misroute opens the actual homepage instead of searching for a homepage motif',async t=>{
  const f=await fixture(t,{providerTools:true});await openCurrent(f);await f.say('Take me to the homepage');const out=await f.provider('find_jewellery',{message:'homepage'});
  assert.equal(out.ok,true,JSON.stringify(out));assert.ok(f.d.querySelector('.collection-intro'));assert.equal(f.d.querySelector('.product-layout'),null);assert.equal(f.store.snapshot().currentHandle,'');assertShopperWording(out);assert.equal(f.catalogueCalls().length,0);
  assert.equal(new URL(f.w.location.href).searchParams.has('product'),false);
});

test('native provider exact quantity and add update actual chosen native controls and the persistent test bag',async t=>{
  const f=await fixture(t,{providerTools:true});await openCurrent(f);await exactSelection(f);await f.say('Set quantity to two');let out=await f.provider('control_storefront',{type:'product-quantity',handle,quantity:2});
  assert.equal(out.ok,true,JSON.stringify(out));assert.equal(f.d.querySelector('input[aria-label="Quantity of this exact piece"]').value,'2');assert.equal(f.store.snapshot().productControls.quantity,2);assertShopperWording(out);
  const chosen=f.store.snapshot().productControls.variantId;await f.say('Add this piece to my cart');out=await f.provider('control_storefront',{type:'add',handle});
  assert.equal(out.ok,true,JSON.stringify(out));assert.equal(out.cartChanged,true);const bag=f.cart();assert.equal(bag.length,1);assert.equal(bag[0].variantId,chosen.split('/').pop());assert.equal(bag[0].quantity,2);assert.equal(bag[0].variant,'14k Solid Gold / 16 inches');assert.equal(f.d.querySelector('#bag-count').textContent,'2');assertShopperWording(out);
});

test('native provider cart synonym, exact line quantity and removal operate the rendered cart',async t=>{
  const f=await fixture(t,{providerTools:true});await openCurrent(f);await exactSelection(f);await f.store.execute({type:'product-quantity',handle,quantity:2});await f.say('Add this piece to my cart');assert.equal((await f.provider('control_storefront',{type:'add',handle})).ok,true);
  await f.say('Open my cart');let out=await f.provider('control_storefront',{type:'scroll',section:'bag'});assert.equal(out.ok,true,JSON.stringify(out));assert.equal(f.store.snapshot().pageKind,'bag');assert.ok(f.d.querySelector('.bag-page'));assertShopperWording(out);
  const line=f.store.snapshot().bagControls.lines[0];await f.say('Set the quantity of the first item in my cart to three');out=await f.provider('control_storefront',{type:'bag-quantity',lineId:line.lineId,quantity:3});
  assert.equal(out.ok,true,JSON.stringify(out));assert.equal(f.cart()[0].quantity,3);assert.equal(f.d.querySelector('[data-bag-line] input').value,'3');assert.equal(f.store.snapshot().bagControls.itemCount,3);assertShopperWording(out);
  await f.say('Remove the first item from my cart');out=await f.provider('control_storefront',{type:'bag-remove',lineId:line.lineId});assert.equal(out.ok,true,JSON.stringify(out));assert.deepEqual(f.cart(),[]);assert.equal(f.d.querySelector('[data-bag-line]'),null);assert.equal(f.store.snapshot().bagControls.itemCount,0);assertShopperWording(out);
});

test('normal native finalized shopping flow selects options, quantity, adds, edits one exact cart line and removes it',async t=>{
  const f=await fixture(t);await openCurrent(f);for(const text of ['Open the metal menu','Select 14k Solid Gold','Select 16 inches','Set quantity to two','Add this piece to my cart','Take me to my cart'])await f.say(text);
  assert.equal(f.store.snapshot().pageKind,'bag');assert.equal(f.cart().length,1);assert.equal(f.cart()[0].variant,'14k Solid Gold / 16 inches');assert.equal(f.cart()[0].quantity,2);assert.equal(f.d.querySelector('[data-bag-line] input').value,'2');
  await f.say('Set the quantity of the first item in my cart to three');assert.equal(f.cart()[0].quantity,3);assert.equal(f.d.querySelector('[data-bag-line] input').value,'3');assertShopperWording(f.result());
  await f.say('Remove the first item from my cart');assert.deepEqual(f.cart(),[]);assert.equal(f.d.querySelector('[data-bag-line]'),null);assertShopperWording(f.result());
});

test('native engraving turns select the actual published choice and write, replace and clear literal private text',async t=>{
  const f=await fixture(t,{engraving:true});await openCurrent(f);for(const text of ['Select 14k Solid Gold','Select 16 inches','Select Engraved','Set engraving text to PRIVATE ELLA 45'])await f.say(text);
  const field=f.d.querySelector('textarea[name="test-engraving-preview"]');assert.equal(field?.value,'PRIVATE ELLA 45');assert.equal(f.selected().Engraving,'Engraved');assertShopperWording(f.result());
  await f.say('Change engraving text to PRIVATE FINN 45');assert.equal(field.value,'PRIVATE FINN 45');assertShopperWording(f.result());
  await f.say('Clear my engraving text');assert.equal(field.value,'');assert.equal(f.selected().Engraving,'Engraved');assertShopperWording(f.result());
  await f.say('Select None');assert.equal(f.selected().Engraving,'None');assert.equal(f.store.snapshot().productControls.selectionStatus,'ready');assertShopperWording(f.result());
  assert.doesNotMatch(f.w.sessionStorage.getItem('brites-concierge-v1')||'',/PRIVATE (?:ELLA|FINN) 45/);assert.doesNotMatch(JSON.stringify(f.receipts()),/PRIVATE (?:ELLA|FINN) 45/);
});

test('unrelated finalized native information cannot authorize the model to change product choices or add a cart line',async t=>{
  const f=await fixture(t,{providerTools:true});await openCurrent(f);await exactSelection(f);const before=clone(f.store.snapshot().productControls);await f.say('Tell me what the otter motif represents');
  const out=await f.provider('control_storefront',{type:'add',handle});assert.notEqual(out.ok,true);assert.deepEqual(f.cart(),[]);assert.deepEqual(clone(f.store.snapshot().productControls.selectedOptions),before.selectedOptions);assert.equal(f.controls.length,0);assertShopperWording(out);
});

test('old finalized input and stale provider response cannot revive a cart request after the shopper changes their mind',async t=>{
  const f=await fixture(t,{providerTools:true});await openCurrent(f);await exactSelection(f);const old=await f.say('Add this piece to my cart'),issued=f.responses().at(-1);await f.say('Tell me about this piece');
  f.emit({type:'response.created',response:{id:'native45-stale-response',metadata:issued.response.metadata}});f.emit({type:'response.function_call_arguments.done',response_id:'native45-stale-response',call_id:'native45-stale-call',name:'control_storefront',arguments:JSON.stringify({type:'add',handle})});f.commit(old);f.final(old,'Add this piece to my cart');await settle();
  assert.deepEqual(f.cart(),[]);assert.equal(f.controls.length,0);assert.equal(f.calls.length,0);assert.equal(f.d.querySelector('#bag-count').textContent,'0');
});

test('native cart ordinal changes preserve the other independently chosen line and repeat events cannot double-add',async t=>{
  const f=await fixture(t);await openCurrent(f);for(const text of ['Select 14k Solid Gold','Select 16 inches','Set quantity to two','Add this piece to my cart','Select 18 inches','Set quantity to one'])await f.say(text);
  const added=await f.say('Add this piece to my cart');assert.equal(f.cart().length,2);assert.equal(f.cart()[0].variant,'14k Solid Gold / 16 inches');assert.equal(f.cart()[0].quantity,2);assert.equal(f.cart()[1].variant,'14k Solid Gold / 18 inches');assert.equal(f.cart()[1].quantity||1,1);
  f.final(added,'Add this piece to my cart');f.commit(added);await settle();assert.equal(f.cart().length,2,'replayed final ASR cannot duplicate the second line');
  await f.say('Take me to my cart');const lines=clone(f.store.snapshot().bagControls.lines);assert.equal(lines.length,2);assert.notEqual(lines[0].lineId,lines[1].lineId);
  await f.say('Set the quantity of the second item in my cart to four');assert.equal(f.cart()[0].quantity,2);assert.equal(f.cart()[1].quantity,4);assert.equal(f.store.snapshot().bagControls.lines[0].lineId,lines[0].lineId);assert.equal(f.store.snapshot().bagControls.lines[1].lineId,lines[1].lineId);assert.equal(f.store.snapshot().bagControls.itemCount,6);assertShopperWording(f.result());
  await f.say('Remove the first item from my cart');assert.equal(f.cart().length,1);assert.equal(f.cart()[0].variant,'14k Solid Gold / 18 inches');assert.equal(f.cart()[0].quantity,4);assert.equal(f.store.snapshot().bagControls.lines[0].lineId,lines[1].lineId);assert.equal(f.d.querySelector('[data-bag-line] input').value,'4');assertShopperWording(f.result());
});

test('provider handle drift cannot substitute another product for the finalized current-product quantity request',async t=>{
  const f=await fixture(t,{providerTools:true});await openCurrent(f);await exactSelection(f);await f.say('Set quantity to two');
  const out=await f.provider('control_storefront',{type:'product-quantity',handle:'native-flow-1',quantity:2});
  assert.equal(out.ok,true,JSON.stringify(out));assert.equal(f.store.snapshot().currentHandle,handle);assert.equal(f.store.snapshot().productControls.quantity,2);assert.equal(f.d.querySelector('input[aria-label="Quantity of this exact piece"]').value,'2');assert.deepEqual(f.selected(),{'Metal Choice':'14k Solid Gold','Chain Length':'16 inches'});assert.equal(f.controls.length,1);assert.equal(f.controls[0].handle,handle);assertShopperWording(out);
});

test('a host failure gives safe shopper guidance rather than exposing hidden diagnostic reason text',async t=>{
  const f=await fixture(t,{providerTools:true});await openCurrent(f);const actual=f.w.BritesSandboxStorefront;
  f.w.BritesSandboxStorefront={...actual,execute:async()=>({ok:false,reason:'Backend adapter postcondition failed: motif=gold; request_id=private-45. No further action was completed.'})};
  await f.say('Set quantity to two');const out=await f.provider('control_storefront',{type:'product-quantity',handle,quantity:2});
  assert.notEqual(out.ok,true);assert.equal(f.store.snapshot().productControls.quantity,1);assert.deepEqual(f.cart(),[]);assertShopperWording(out);assert.doesNotMatch(JSON.stringify(out.modelReceipt),/private-45|postcondition|request_id|No further action/i);
});

test('native exact engraved preview add preserves private wording and cart choice edits preserve the stable line and quantity',async t=>{
  const f=await fixture(t,{engraving:true});await openCurrent(f);
  for(const text of ['Select 14k Solid Gold','Select 16 inches','Select Engraved','Set engraving text to PRIVATE ELLA CART 45','Set quantity to two','Add this piece to my cart','Take me to my cart'])await f.say(text);
  let rows=f.cart();assert.equal(rows.length,1);assert.equal(rows[0].variant,'14k Solid Gold / 16 inches / Engraved');assert.equal(rows[0].quantity,2);assert.equal(rows[0].engravingPreview,'PRIVATE ELLA CART 45');assert.equal(f.store.snapshot().pageKind,'bag');
  const line=f.store.snapshot().bagControls.lines[0],lineId=line.lineId;assert.equal(line.engravingControls.hasText,true);assert.equal(f.d.querySelector('[data-bag-line] [data-bag-engraving]').value,'PRIVATE ELLA CART 45');assert.doesNotMatch(JSON.stringify(line),/PRIVATE ELLA CART 45/);assert.doesNotMatch(JSON.stringify(Voice.publicContext(f.config.getContext())),/PRIVATE ELLA CART 45/);
  await f.say('Change the first item in my cart to 18 inches');rows=f.cart();assert.equal(rows[0].variant,'14k Solid Gold / 18 inches / Engraved');assert.equal(rows[0].quantity,2);assert.equal(rows[0].engravingPreview,'PRIVATE ELLA CART 45');assert.equal(f.store.snapshot().bagControls.lines[0].lineId,lineId);assertShopperWording(f.result());
  const nativeRow=f.d.querySelector('[data-bag-line]');assert.ok(nativeRow);assert.equal(nativeRow.querySelector('[data-bag-option="Chain Length"]').value,'18 inches');assert.equal(nativeRow.querySelector('[data-bag-option="Engraving"]').value,'Engraved');
  await f.say('Set engraving for the first item in my cart to PRIVATE REN CART 45');assert.equal(f.cart()[0].engravingPreview,'PRIVATE REN CART 45');assert.equal(f.d.querySelector('[data-bag-line] [data-bag-engraving]').value,'PRIVATE REN CART 45');assert.equal(f.store.snapshot().bagControls.lines[0].lineId,lineId);assertShopperWording(f.result());
  await f.say('Clear engraving text for the first item in my cart');assert.equal(f.cart()[0].engravingPreview||'','');assert.equal(f.d.querySelector('[data-bag-line] [data-bag-engraving]').value,'');assert.equal(f.cart()[0].variant,'14k Solid Gold / 18 inches / Engraved');assertShopperWording(f.result());
  await f.say('Select None for engraving on the first item in my cart');assert.equal(f.cart()[0].variant,'14k Solid Gold / 18 inches / None');assert.equal(f.d.querySelector('[data-bag-line] [data-bag-option="Engraving"]').value,'None');assert.equal(f.cart()[0].quantity,2);assert.equal(f.store.snapshot().bagControls.lines[0].lineId,lineId);assertShopperWording(f.result());
  assert.doesNotMatch(JSON.stringify(f.receipts()),/PRIVATE (?:ELLA|REN) CART 45/);assert.doesNotMatch(f.w.sessionStorage.getItem('brites-concierge-v1')||'',/PRIVATE (?:ELLA|REN) CART 45/);
});

test('a native cart-only option edit preserves the unrelated first line, published metal and exact chosen quantity',async t=>{
  const f=await fixture(t);await openCurrent(f);for(const text of ['Select 14k Solid Gold','Select 16 inches','Set quantity to two','Add this piece to my cart','Select 14k Gold Filled','Select 18 inches','Set quantity to three','Add this piece to my cart','Take me to my cart'])await f.say(text);
  const before=clone(f.cart()),ids=f.store.snapshot().bagControls.lines.map(line=>line.lineId);assert.equal(before.length,2);
  await f.say('Change the second item in my cart to 16 inches');const after=f.cart();assert.deepEqual(after[0],before[0]);assert.equal(after[1].variant,'14k Gold Filled / 16 inches');assert.equal(after[1].quantity,3);assert.deepEqual(f.store.snapshot().bagControls.lines.map(line=>line.lineId),ids);assert.equal(f.store.snapshot().bagControls.itemCount,5);assertShopperWording(f.result());
  const row=[...f.d.querySelectorAll('[data-bag-line]')].find(node=>node.dataset.bagLine===ids[1]);assert.ok(row);assert.equal(row.querySelector('[data-bag-option="Metal Choice"]').value,'14k Gold Filled');assert.equal(row.querySelector('[data-bag-option="Chain Length"]').value,'16 inches');assert.equal(row.querySelector('input[type="number"]').value,'3');
});

test('an engraved native preview remains a saved cart choice and cannot silently cross the shop-review checkout boundary',async t=>{
  const f=await fixture(t,{engraving:true});await openCurrent(f);for(const text of ['Select 14k Solid Gold','Select 16 inches','Select Engraved','Set engraving text to PRIVATE REVIEW 45','Add this piece to my cart','Take me to my cart'])await f.say(text);
  const before=clone(f.cart());assert.equal(before.length,1);assert.equal(before[0].engravingPreview,'PRIVATE REVIEW 45');await f.say('Open checkout');
  assert.equal(f.store.snapshot().pageKind,'bag');assert.equal(f.d.querySelector('.checkout-page'),null);assert.deepEqual(f.cart(),before);assert.notEqual(f.result()?.ok,true);assert.match(customerText(f.result()),/shop|studio|review/i);assertShopperWording(f.result());assert.doesNotMatch(JSON.stringify(f.receipts()),/PRIVATE REVIEW 45/);
});

test('an opted-in native widget renews a confirmed preview-session expiry exactly once while preserving chosen product and cart state',async t=>{
  const f=await fixture(t);await openCurrent(f);for(const text of ['Select 14k Solid Gold','Select 16 inches','Set quantity to two','Add this piece to my cart'])await f.say(text);
  const before={cart:clone(f.cart()),product:clone(f.store.snapshot().productControls),controls:f.controls.length,receipts:f.receipts().length};assert.equal(f.startCount(),1);const oldStopped=f.config.onStopped;
  const stopped=await f.client.stop('limit');assert.equal(stopped.serverStopped,true);await until(()=>f.startCount()===2&&['listening','thinking'].includes(f.client.state),()=>JSON.stringify({startCount:f.startCount(),state:f.client.state,status:f.root.querySelector('.status')?.textContent,error:f.client.lastError?.message}));
  if(f.client.state==='thinking'){
    const continuation=f.responses().at(-1);assert.equal(continuation.response.tool_choice,'none');assert.match(continuation.response.instructions,/reply|continue/i);
    f.emit({type:'response.created',response:{id:'native45-renewed-reply',metadata:continuation.response.metadata}});f.emit({type:'response.done',response:{id:'native45-renewed-reply',status:'completed',output:[]}});await settle();
  }
  assert.equal(f.client.state,'listening');
  assert.deepEqual(f.cart(),before.cart);assert.deepEqual(clone(f.store.snapshot().productControls.selectedOptions),before.product.selectedOptions);assert.equal(f.store.snapshot().productControls.quantity,2);assert.equal(f.controls.length,before.controls);assert.equal(f.receipts().length,before.receipts);assert.equal(f.root.querySelector('button[aria-pressed="true"]')?.textContent,'End voice');
  oldStopped('limit',{serverStopped:true});await settle();assert.equal(f.startCount(),2,'duplicate expiry callback cannot issue another native session');
});

test('manual End voice during an expiring native session cancels renewal and preserves the current shopping choices',async t=>{
  const f=await fixture(t);await openCurrent(f);await exactSelection(f);const before=clone(f.store.snapshot().productControls),closing=f.client.stop('limit');
  const end=[...f.root.querySelectorAll('button')].find(button=>button.textContent==='End voice');assert.ok(end);end.click();await closing;await settle();
  assert.equal(f.startCount(),1);assert.equal(f.client.state,'idle');assert.deepEqual(clone(f.store.snapshot().productControls.selectedOptions),before.selectedOptions);assert.deepEqual(f.cart(),[]);assert.equal(f.root.querySelector('button[aria-pressed="true"]')?.textContent==='End voice',false);
});

test('hidden or closed native widget cannot renew an expiring session or replay a shopper cart action',async t=>{
  for(const boundary of ['hidden','closed']){
    const f=await fixture(t);await openCurrent(f);await exactSelection(f);await f.say('Add this piece to my cart');const before=clone(f.cart()),count=f.controls.length,closing=f.client.stop('limit');
    if(boundary==='hidden')f.hide();else f.w.BritesConcierge.close();await closing;await settle();assert.equal(f.startCount(),1,boundary);assert.equal(f.client.state,'idle',boundary);assert.deepEqual(f.cart(),before,boundary);assert.equal(f.controls.length,count,boundary);
  }
});

test('a native session expiry without confirmed provider stop pauses the widget instead of starting another session',async t=>{
  const f=await fixture(t,{serverStopConfirmed:false});await openCurrent(f);await exactSelection(f);const before=clone(f.store.snapshot().productControls);assert.equal(f.startCount(),1);
  const stopped=await f.client.stop('limit');assert.equal(stopped.serverStopped,false);await settle();assert.equal(f.startCount(),1);assert.equal(f.client.state,'idle');assert.deepEqual(clone(f.store.snapshot().productControls.selectedOptions),before.selectedOptions);assert.equal(f.controls.length,0);assert.deepEqual(f.cart(),[]);assert.match(f.root.textContent,/Voice paused|Talk to me/i);
});

test('a trusted first renewal 429 retries once and reconnects the actual native widget without replaying its cart action',async t=>{
  const f=await fixture(t,{renewalRateLimits:1});await openCurrent(f);for(const text of ['Select 14k Solid Gold','Select 16 inches','Set quantity to two','Add this piece to my cart'])await f.say(text);
  const before={cart:clone(f.cart()),choices:clone(f.store.snapshot().productControls.selectedOptions),controls:f.controls.length,receipts:f.receipts().length};assert.equal(f.startCount(),1);
  const stopped=await f.client.stop('limit');assert.equal(stopped.serverStopped,true);await until(()=>f.startCount()===3&&['listening','thinking'].includes(f.client.state),()=>JSON.stringify({starts:f.startCount(),state:f.client.state,error:f.client.lastError,status:f.root.querySelector('.status')?.textContent}));
  if(f.client.state==='thinking'){
    const continuation=f.responses().at(-1);assert.equal(continuation.response.tool_choice,'none');f.emit({type:'response.created',response:{id:'native45-rate-renewed-reply',metadata:continuation.response.metadata}});f.emit({type:'response.done',response:{id:'native45-rate-renewed-reply',status:'completed',output:[]}});await settle();
  }
  assert.equal(f.client.state,'listening');assert.equal(f.startCount(),3);assert.deepEqual(f.cart(),before.cart);assert.deepEqual(clone(f.store.snapshot().productControls.selectedOptions),before.choices);assert.equal(f.store.snapshot().productControls.quantity,2);assert.equal(f.controls.length,before.controls);assert.equal(f.receipts().length,before.receipts);
  assert.equal(f.requests.filter(r=>r.url.pathname==='/api/concierge-voice'&&JSON.parse(r.init.body||'{}').action==='stop').length,1,'renewal starts only after the original call was actually stopped');
});

test('repeated trusted native renewal 429 responses stop after two retries and preserve the actual cart and choices',async t=>{
  const f=await fixture(t,{renewalRateLimits:Infinity});await openCurrent(f);for(const text of ['Select 14k Solid Gold','Select 16 inches','Add this piece to my cart'])await f.say(text);
  const before={cart:clone(f.cart()),choices:clone(f.store.snapshot().productControls.selectedOptions),controls:f.controls.length,receipts:f.receipts().length};await f.client.stop('limit');
  await until(()=>f.startCount()===4&&f.client.state==='idle'&&f.root.querySelector('button[data-retry="true"]'),()=>JSON.stringify({starts:f.startCount(),state:f.client.state,error:f.client.lastError,status:f.root.querySelector('.status')?.textContent}));
  await new Promise(resolve=>setTimeout(resolve,20));assert.equal(f.startCount(),4,'one renewal plus at most two retries must end the automatic retry chain');assert.deepEqual(f.cart(),before.cart);assert.deepEqual(clone(f.store.snapshot().productControls.selectedOptions),before.choices);assert.equal(f.controls.length,before.controls);assert.equal(f.receipts().length,before.receipts);assert.match(f.root.querySelector('.status').textContent,/wait|voice|retry|try|type/i);
});

test('actual End voice while a trusted renewal retry is scheduled cancels the timer and preserves the cart',async t=>{
  const f=await fixture(t,{renewalRateLimits:1});await openCurrent(f);for(const text of ['Select 14k Solid Gold','Select 16 inches','Add this piece to my cart'])await f.say(text);
  const before={cart:clone(f.cart()),controls:f.controls.length},observer=new f.w.MutationObserver(()=>{
    if(!/Voice is busy/i.test(f.root.querySelector('.status')?.textContent||''))return;
    const end=[...f.root.querySelectorAll('button')].find(button=>button.textContent==='End voice');if(end){observer.disconnect();end.click();cancelled=true;}
  });let cancelled=false;observer.observe(f.root,{subtree:true,childList:true,characterData:true});t.after(()=>observer.disconnect());
  await f.client.stop('limit');await until(()=>cancelled,()=> 'The visible reconnect notice must offer End voice before the retry fires');await settle();await new Promise(resolve=>setTimeout(resolve,20));
  assert.equal(f.startCount(),2,'the cancelled retry must never create the third native call');assert.equal(f.client.state,'idle');assert.deepEqual(f.cart(),before.cart);assert.equal(f.controls.length,before.controls);assert.equal(f.root.querySelector('button[aria-pressed="true"]')?.textContent==='End voice',false);
});
