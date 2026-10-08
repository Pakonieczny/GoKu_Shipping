'use strict';
// Actual native client, production adapter, bridge and widget with synthetic
// WebRTC and HTTP. No microphone, provider, production cart or payment is used.
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs');
const {JSDOM,VirtualConsole}=require('jsdom');
const Voice=require('../../brites-concierge-voice.js');
const Server=require('../../netlify/functions/_britesConciergeVoice.js');
const Adapter=require('../../brites-shopify-storefront-adapter.js');
const Bridge=require('../../brites-storefront-bridge.js');
const Catalogue=require('../../brites-catalogue-intents.js');
const source=Object.fromEntries(['brites-concierge.js','brites-storefront-bridge.js','brites-catalogue-intents.js','brites-concierge-shopping-guide.js'].map(name=>[name,fs.readFileSync(require.resolve('../../'+name),'utf8')]));
const SDP='v=0\r\nm=audio 9 UDP/TLS/RTP/SAVPF 111\r\na=candidate:1 1 UDP 2122260223 192.0.2.10 50000 typ host\r\n';
const clone=value=>JSON.parse(JSON.stringify(value));
const settle=async()=>{for(let n=0;n<5;n++)await new Promise(setImmediate);};
const deferred=()=>{let resolve;const promise=new Promise(done=>resolve=done);return {promise,resolve};};
function rawPiece(){return {id:430,title:'Butterfly Charm Necklace',handle:'butterfly-charm-necklace',type:'Necklace',description:'The published butterfly charm measures 14 mm tall and 12 mm wide, on your selected chain. Sterling Silver and Gold Filled are published metal choices.',options:['Metal Choice','Chain Length'],variants_count:4,variants:['Sterling Silver','Gold Filled'].flatMap((metal,m)=>['16 inches','18 inches'].map((length,l)=>({id:4301+m*2+l,title:metal+' / '+length,price:5000+m*2000+l*500,available:true,options:[metal,length]})))};}
function checkedPiece(raw){const p=Adapter.projectProduct(raw,'USD',raw.variants_count);p.url='https://britesjewelry.com/products/'+p.handle;p.options=p.optionGroups;return p;}
const HANDLE='butterfly-charm-necklace',VARIANT='gid://shopify/ProductVariant/4304';
function controls(p=checkedPiece(rawPiece()),{missing=false}={}){const v=p.variants.at(-1);return {handle:p.handle,productId:p.id,quantity:3,variantId:missing?null:v.id,selectionStatus:missing?'choosing':'ready',optionsOpen:missing,openedOption:missing?'Metal Choice':null,optionGroups:p.optionGroups.map(g=>({name:g.name,values:g.values})),selectedOptions:missing?[clone(v.options[1])]:clone(v.options),selectedVariant:missing?undefined:{...clone(v),currency:p.currency},itemTotalPrice:missing?undefined:225,requiredOptions:missing?['Metal Choice']:[],requiredOptionGroups:missing?[clone(p.optionGroups[0])]:[]};}
function knowledgeContext(){const p=checkedPiece(rawPiece()),checkedAt=Date.now();return {controlVersion:1,pageKind:'product',currentHandle:p.handle,focusedHandle:'unrelated-old-piece',visiblePieces:[{id:p.id,handle:p.handle,title:p.title}],productControls:controls(p),currentProduct:{...clone(p),checkedAt,fromCache:true,factsSource:{kind:'checked_storefront_inventory',url:p.url,checkedAt,cached:true},selectedVariant:{...clone(p.variants.at(-1)),currency:'USD'},quantity:3,itemTotalPrice:225}};}

async function productionFixture(t,{selected=true,missingLength=false,refresh=true,voice=true,quantity=3,misrouteReview=false,misrouteAdd=false}={}){
  const errors=[],vc=new VirtualConsole();vc.on('jsdomError',error=>errors.push(error));
  const markup='<!doctype html><body><main id="MainContent"><div id="shopify-section-product-template"><h1 class="pi__title">Butterfly Charm Necklace</h1><span id="bjPrice">$75.00</span><form id="bjForm"><div id="bjMetals" data-idx="1"><button type="button" data-vi="0">Sterling Silver</button><button type="button" data-vi="1"'+(selected?' class="on"':'')+'>Gold Filled</button></div><div class="bjselx"><select class="bjOptSel" data-idx="2"><option>16 inches</option><option selected>18 inches</option></select><button type="button" class="bjselx__btn" aria-haspopup="listbox" aria-expanded="false">Chain length</button></div><input id="bjQty" type="number" value="'+quantity+'"><button type="button" id="bjAdd">Add to bag</button></form><details><summary>Product Details</summary><div class="bd">The charm measures 14 mm tall and 12 mm wide.</div></details></div></main></body>';
  const dom=new JSDOM(markup,{url:'https://britesjewelry.com/products/'+HANDLE,runScripts:'outside-only',pretendToBeVisual:true,virtualConsole:vc}),w=dom.window,d=w.document;
  const raw=rawPiece(),requests=[],controls=[],sent=[],hookCalls=[],scrolled=[];let apiPiece=checkedPiece(raw),cart={currency:'USD',item_count:0,items:[]},channel,client,config,turn=0,hidden=false,productGate=null,addGate=null,serverAcceptedAbort=false,readbackConfirmed=true,cartFailAfterPost=false;
  Object.defineProperty(d,'hidden',{get:()=>hidden});w.HTMLElement.prototype.scrollIntoView=function(){scrolled.push(this);};w.matchMedia=()=>({matches:false,addEventListener(){}});w.HTMLMediaElement.prototype.play=async function(){};w.HTMLMediaElement.prototype.pause=function(){};
  if(missingLength)d.querySelector('select.bjOptSel').selectedIndex=-1;
  w.Shopify={routes:{root:'/'},currency:{active:'USD'}};
  d.querySelectorAll('#bjMetals button').forEach(button=>button.addEventListener('click',()=>{d.querySelectorAll('#bjMetals button').forEach(other=>other.classList.toggle('on',other===button));}));
  d.querySelector('.bjselx__btn').addEventListener('click',event=>{const button=event.currentTarget;button.setAttribute('aria-expanded',String(button.getAttribute('aria-expanded')!=='true'));});
  const fetch=async(rawURL,init={})=>{
    const url=new URL(rawURL,w.location.href),body=typeof init.body==='string'?JSON.parse(init.body):null;requests.push({url,init,body});let value;
    if(url.pathname==='/cart/add.js'){
      if(addGate)await addGate.promise;
      if(init.signal?.aborted&&!serverAcceptedAbort)throw Object.assign(Error('Synthetic abort'),{name:'AbortError'});
      const items=body.items.map((i,n)=>{const v=raw.variants.find(v=>v.id===i.id);return {key:String(i.id)+':request-'+(cart.items.length+n+1),product_id:raw.id,variant_id:i.id,product_title:raw.title,variant_title:v.title,quantity:i.quantity,price:v.price,final_price:v.price,properties:i.properties};});
      if(readbackConfirmed){cart.items.push(...items);cart.item_count=cart.items.reduce((sum,line)=>sum+line.quantity,0);}value={items};
    }else if(url.pathname==='/cart.js'){
      if(cartFailAfterPost&&requests.some(r=>r.url.pathname==='/cart/add.js'))throw Error('Synthetic readback failure');value=cart;
    }else if(url.pathname==='/products/'+HANDLE+'.js')value=raw;
    else if(url.pathname==='/api/growth/product'){
      if(productGate)await productGate.promise;
      if(init.signal?.aborted)throw Object.assign(Error('Synthetic abort'),{name:'AbortError'});value={live:true,checkedAt:Date.now(),product:apiPiece};
    }else if(url.pathname==='/api/concierge-voice'){const action=body.action;value=action==='capabilities'?{enabled:true,nativeAudio:true}:action==='start'?{sdp:SDP,stopToken:'synthetic-shopping43',maxDurationMs:120000}:{stopped:true};}
    else if(url.pathname==='/api/growth/events')value={ok:true};
    else if(url.pathname==='/api/concierge')value={live:true,reply:'Synthetic ordinary conversation.',products:[],meanings:[],preferences:{},preserveSelection:true};
    else throw Error('Unexpected synthetic route '+url.pathname);
    return {ok:true,status:200,json:async()=>clone(value)};
  };w.fetch=fetch;
  const store=Adapter.create({window:w,root:'/',theme:'brites-v1',currency:'USD',product:raw,variantCount:raw.variants.length,fetch,navigate:()=>{}});assert.ok(store);
  if(refresh){assert.equal(await store.refresh(),true);await settle();}
  w.BritesStorefrontAdapter={...store,get capabilities(){return store.capabilities;},async execute(action,options){controls.push({action:clone(action),signal:options?.signal});return store.execute(action,options);}};
  class Peer{constructor(){this.iceGatheringState='complete';}addTrack(){}close(){}createDataChannel(){channel={readyState:'connecting',send:value=>sent.push(JSON.parse(value)),close(){}};return channel;}async createOffer(){return {type:'offer',sdp:SDP};}async setLocalDescription(value){this.localDescription=value;}async setRemoteDescription(){channel.readyState='open';channel.onopen();}}
  const runtime={document:d,location:w.location,navigator:{mediaDevices:{getUserMedia:async()=>{const track={readyState:'live',stop(){this.readyState='ended';},addEventListener(){},removeEventListener(){}};return {getTracks:()=>[track],getAudioTracks:()=>[track]};}}},RTCPeerConnection:Peer,AbortController:w.AbortController,fetch,setTimeout:w.setTimeout.bind(w),clearTimeout:w.clearTimeout.bind(w),addEventListener:w.addEventListener.bind(w),removeEventListener:w.removeEventListener.bind(w)};
  w.BritesConciergeVoice={publicContext:Voice.publicContext,create(options){config=options;client=Voice.create({...options,runtime,greeting:false});return client;}};
  w.BritesConciergeAvatar={create(){return {setState(){},setEmotion(){},setVisible(){},setPaused(){},triggerGreeting(){},clearFocus(){},focusProduct(){},setLevel(){},setSpeechSignal(){},clearProduct(){},showProduct(){},cancelPerformance(){},setFloating(){},cue(){}};}};
  for(const name of ['brites-catalogue-intents.js','brites-concierge-shopping-guide.js','brites-storefront-bridge.js'])w.eval(source[name]);
  const script=d.createElement('script');script.src='/brites-concierge.js';script.dataset.sandbox='false';Object.defineProperty(d,'currentScript',{get:()=>script});w.eval(source['brites-concierge.js']);const root=d.querySelector('brites-concierge').shadowRoot;
  // Hook wrapper records authority and exact arguments without fabricating it.
  const hooks=w.BritesConciergeShopifyControls;w.BritesConciergeShopifyControls={...hooks,async add(args){hookCalls.push(args);return misrouteAdd?hooks.reviewAdd(args):hooks.add(args);},async reviewAdd(args){return misrouteReview?hooks.add(args):hooks.reviewAdd(args);}};
  w.BritesConcierge.open({focus:false});
  if(voice){[...root.querySelectorAll('button')].find(b=>b.textContent==='Talk to me').click();await settle();root.querySelector('script[src$="brites-concierge-voice.js"]')?.dispatchEvent(new w.Event('load'));await settle();assert.equal(client.state,'listening');}
  else {[...root.querySelectorAll('button')].find(b=>b.textContent==='Type instead').click();await settle();}
  const emit=event=>channel.onmessage({data:JSON.stringify(event)}),responses=()=>sent.filter(e=>e.type==='response.create'),receipts=()=>sent.filter(e=>e.item?.content?.[0]?.text?.startsWith('Host-completed result'));
  function begin(){const itemId='shopping-voice43-'+(++turn);emit({type:'input_audio_buffer.speech_started',item_id:itemId});emit({type:'input_audio_buffer.speech_stopped',item_id:itemId});return {itemId,version:turn+1};}
  const commit=value=>emit({type:'input_audio_buffer.committed',item_id:value.itemId}),final=(value,text)=>emit({type:'conversation.item.input_audio_transcription.completed',item_id:value.itemId,transcript:text});
  async function say(text){const input=begin();commit(input);final(input,text);await settle();return input;}
  const result=()=>{const text=receipts().at(-1)?.item.content[0].text;return text?JSON.parse(text.slice(text.indexOf('{'))).result:null;};
  t.after(async()=>{w.BritesConcierge?.close();await client?.dispose();store.destroy();w.close();assert.deepEqual(errors,[]);});
  return {w,d,root,store,requests,controls,sent,hookCalls,scrolled,raw,errors,emit,responses,receipts,begin,commit,final,say,result,cart:()=>clone(cart),get config(){return config;},get client(){return client;},postCount:()=>requests.filter(r=>r.url.pathname==='/cart/add.js').length,api(value){apiPiece=value;},setCartCurrency(value){cart.currency=value;},deferProduct(){productGate=deferred();return productGate;},deferAdd({acceptAborted=false}={}){serverAcceptedAbort=acceptAborted;addGate=deferred();return addGate;},uncertain({failReadback=false}={}){readbackConfirmed=false;cartFailAfterPost=failReadback;},async changeMetal(value){d.querySelectorAll('#bjMetals button').forEach(button=>button.classList.toggle('on',button.textContent===value));d.querySelector('#bjForm').dispatchEvent(new w.Event('change',{bubbles:true}));await settle();},async moveToCart(){w.history.replaceState(null,'','/cart');d.dispatchEvent(new w.CustomEvent('brites-storefront:context'));await settle();},hide(){hidden=true;d.dispatchEvent(new w.Event('visibilitychange'));}};
}

test('browser/server/tool schema admit add only with exact declared product and optional exact variant',()=>{
  const tool=Server.sessionConfig().tools.find(t=>t.name==='control_storefront');assert.ok(tool.parameters.properties.type.enum.includes('add'));
  for(const value of [{type:'add',handle:HANDLE},{type:'add',handle:HANDLE,variantId:VARIANT}])for(const implementation of [Voice,Server,Adapter])assert.deepEqual(implementation.validateToolArguments?implementation.validateToolArguments(value,'control_storefront'):clone(implementation.validateAction(value)),value);
  for(const value of [{type:'add'},{type:'add',handle:HANDLE,quantity:2},{type:'add',handle:HANDLE,variantId:'4304'},{type:'add',handle:HANDLE,reviewAuthority:{}},{type:'add',handle:HANDLE,url:'https://example.com'}]){assert.equal(Voice.validateToolArguments(value,'control_storefront'),null);assert.equal(Server.validateToolArguments(value,'control_storefront'),null);assert.equal(Adapter.validateAction(value),null);}
});

test('missing choices and literal option groups survive bridge and native public context without invented selection',()=>{
  const context={controlVersion:1,pageKind:'product',currentHandle:HANDLE,productControls:controls(undefined,{missing:true})};
  const projected=Voice.publicContext(Bridge.sanitizeSnapshot(context));assert.equal(projected.productControls.selectionStatus,'choosing');assert.deepEqual(projected.productControls.requiredOptions,['Metal Choice']);assert.deepEqual(projected.productControls.requiredOptionGroups,[{name:'Metal Choice',values:['Sterling Silver','Gold Filled']}]);assert.equal(projected.productControls.variantId,null);assert.equal(projected.productControls.selectedVariant,undefined);assert.deepEqual(projected.productControls.selectedOptions,[{name:'Chain Length',value:'18 inches'}]);
});

test('native context byte limits retain the exact selected literal even when outside the first sixty values',()=>{
  const values=Array.from({length:100},(_,i)=>'Option '+String(i).padStart(3,'0')+' '+('x'.repeat(280))),value=values.at(-1),projected=Voice.publicContext({controlVersion:1,pageKind:'product',currentHandle:HANDLE,productControls:{handle:HANDLE,productId:'gid://shopify/Product/430',quantity:3,variantId:VARIANT,selectionStatus:'ready',optionGroups:[{name:'Metal',values}],selectedOptions:[{name:'Metal',value}],selectedVariant:{id:VARIANT,title:'Selected exact option',price:75,currency:'USD',available:true,options:[{name:'Metal',value}]}}});
  assert.ok(Buffer.byteLength(JSON.stringify(projected))<=18000);assert.equal(projected.productControls.selectedOptions[0].value,value);assert.equal(projected.productControls.selectedVariant.options[0].value,value);assert.ok(projected.productControls.optionGroups[0].values.includes(value));assert.equal(projected.productControls.optionsTruncated,true);
});

test('size questions stay factual while butterfly earrings and open assortment remain catalogue discovery',()=>{
  for(const text of ['What size is the charm?','How big is it?','What is the price of this piece?','What material is this piece?'])assert.equal(Catalogue.discovery(text).recognized,false,text);
  assert.equal(Catalogue.discovery('Do you have butterfly earrings?').recognized,true);assert.equal(Catalogue.discovery('What do you have?').recognized,true);
});

test('current native product facts require matching page identity, exact price/options and timestamped owned provenance',()=>{
  const context=knowledgeContext(),out=Voice.publicContext(context);assert.equal(out.currentProduct.handle,HANDLE);assert.equal(out.currentProduct.selectedVariant.id,VARIANT);assert.equal(out.currentProduct.itemTotalPrice,225);assert.match(out.currentProduct.description,/14 mm/);assert.equal(out.currentProduct.factsSource.checkedAt,context.currentProduct.checkedAt);
  for(const damage of [c=>c.currentHandle='another-piece',c=>c.currentProduct.factsSource.checkedAt--,c=>c.currentProduct.factsSource.url='https://example.com/products/'+HANDLE,c=>c.currentProduct.checkedAt=Date.now()+120000]){const changed=clone(context);damage(changed);assert.equal(Voice.publicContext(changed).currentProduct,undefined);}
  for(const damage of [c=>c.productControls.productId='gid://shopify/Product/999',c=>c.productControls.selectedVariant.price++,c=>c.productControls.selectedOptions[0].value='Sterling Silver']){const changed=clone(context);damage(changed);const projected=Voice.publicContext(changed).currentProduct;assert.equal(projected.selectedVariant,undefined);assert.equal(projected.selectionNeedsRefresh,true);}
});

test('native cart projection preserves checked total when current page has no rendered cart lines',()=>{
  const context=Voice.publicContext(Bridge.sanitizeSnapshot({controlVersion:1,pageKind:'product',currentHandle:HANDLE,bagControls:{lines:[],itemCount:3,linesComplete:false,countKnown:true}}));assert.equal(context.bagControls.itemCount,3);assert.equal(context.bagControls.linesComplete,false);
});

test('native cart projection retains large checked count and marks context-limited lines incomplete',()=>{
  const large=Voice.publicContext(Bridge.sanitizeSnapshot({controlVersion:1,pageKind:'product',currentHandle:HANDLE,bagControls:{lines:[],itemCount:1500,countKnown:true,linesComplete:false}}));assert.equal(large.bagControls.itemCount,1500);assert.equal(large.bagControls.linesComplete,false);
  const lines=Array.from({length:50},(_,i)=>({lineId:'4304:synthetic-'+i,productId:'gid://shopify/Product/430',variantId:VARIANT,quantity:1,title:'Product '+i+' '+('T'.repeat(280)),variant:'Option '+('V'.repeat(280))}));
  const bounded=Voice.publicContext({controlVersion:1,pageKind:'bag',bagControls:{lines,itemCount:50,countKnown:true,linesComplete:true}});assert.ok(Buffer.byteLength(JSON.stringify(bounded))<=18000);assert.equal(bounded.bagControls.itemCount,50);assert.ok(bounded.bagControls.lines.length<50);assert.equal(bounded.bagControls.linesTruncated,true);assert.equal(bounded.bagControls.linesComplete,false);
});

test('actual finalized voice add calls the production hook and posts exact selected variant and quantity once',async t=>{
  const h=await productionFixture(t),input=await h.say('Add this piece to my cart');assert.equal(h.controls.length,1,JSON.stringify(h.result()));assert.equal(h.controls[0].action.type,'add');assert.equal(h.hookCalls.length,1,JSON.stringify(h.result()));assert.equal(h.hookCalls[0].variantId,VARIANT);assert.equal(h.hookCalls[0].quantity,3);assert.equal(typeof h.hookCalls[0].reviewAuthority,'object');assert.equal(h.postCount(),1,JSON.stringify(h.result()));
  const post=h.requests.find(r=>r.url.pathname==='/cart/add.js');assert.equal(post.body.items[0].id,4304);assert.equal(post.body.items[0].quantity,3);assert.match(post.body.items[0].properties._BritesConciergeRequest,/^[a-zA-Z0-9_-]{16,100}$/);assert.equal(h.cart().item_count,3);assert.equal(h.result().ok,true);assert.equal(h.result().cartChanged,true);assert.equal(h.result().publicContext.currentHandle,HANDLE);assert.equal(h.result().publicContext.bagControls.itemCount,3);assert.equal(h.responses().at(-1).response.tool_choice,'none');assert.equal(h.root.querySelector('.review'),null);
  h.final(input,'Add this piece to my cart');const response=h.responses().at(-1);h.emit({type:'response.created',response:{id:'shopping43-repeat-response',metadata:response.response.metadata}});h.emit({type:'response.function_call_arguments.done',name:'control_storefront',call_id:'shopping43-repeat-call',response_id:'shopping43-repeat-response',arguments:JSON.stringify({type:'add',handle:HANDLE,variantId:VARIANT})});await settle();assert.equal(h.postCount(),1);assert.equal(h.hookCalls.length,1);assert.equal(h.receipts().length,1);
});

test('real native current-page size questions use checked inventory without search, page controls or new product reads',async t=>{
  const h=await productionFixture(t),before=h.requests.length,revision=h.store.snapshot().contextRevision;
  for(const text of ['What size is the charm?','How big is it?']){await h.say(text);assert.equal(h.result()?.ok,true,JSON.stringify(h.result()));assert.equal(h.result().productFacts.handle,HANDLE);assert.match(h.result().reply,/14 mm/);assert.equal(h.responses().at(-1).response.tool_choice,'none');}
  assert.equal(h.controls.length,0);assert.equal(h.postCount(),0);assert.equal(h.store.snapshot().contextRevision,revision);assert.equal(h.requests.slice(before).filter(r=>['/api/growth/product','/api/concierge'].includes(r.url.pathname)).length,0);const native=Voice.publicContext(h.config.getContext());assert.equal(native.currentProduct.handle,HANDLE);assert.equal(native.currentProduct.selectedVariant.id,VARIANT);assert.equal(native.currentProduct.quantity,3);assert.equal(native.currentProduct.itemTotalPrice,225);
});

test('guessed production direct-add authority fails before network and an already consumed token cannot post again',async t=>{
  const h=await productionFixture(t),before=h.requests.length;assert.equal((await h.w.BritesConciergeShopifyControls.add({product:checkedPiece(h.raw),variantId:VARIANT,quantity:3,requestId:'guessed-request-43',reviewAuthority:{}})).ok,false);assert.equal(h.requests.length,before);
  await h.say('Add this piece to my cart');assert.equal(h.postCount(),1);const consumed=h.hookCalls[0],after=h.requests.length;assert.equal((await h.w.BritesConciergeShopifyControls.add({...consumed})).ok,false);assert.equal(h.requests.length,after);assert.equal(h.postCount(),1);
});

test('a shopper review-only authority cannot be misrouted to the production direct-add hook',async t=>{
  const h=await productionFixture(t,{misrouteReview:true});await h.say('Review adding this piece to my cart');assert.equal(h.controls.length,1);assert.equal(h.controls[0].action.type,'review-add');assert.equal(h.postCount(),0);assert.equal(h.cart().item_count,0);assert.ok(h.result()?.error);assert.notEqual(h.result()?.cartChanged,true);
});

test('an explicit production add authority cannot be reused to prepare an unrelated review operation',async t=>{
  const h=await productionFixture(t,{misrouteAdd:true}),before=h.requests.filter(r=>r.url.pathname==='/api/growth/product').length;await h.say('Add this piece to my cart');assert.equal(h.controls.length,1);assert.equal(h.controls[0].action.type,'add');assert.equal(h.hookCalls.length,1);assert.equal(h.postCount(),0);assert.equal(h.cart().item_count,0);assert.equal(h.root.querySelector('.review'),null);assert.equal(h.requests.filter(r=>r.url.pathname==='/api/growth/product').length,before);assert.ok(h.result()?.error);assert.notEqual(h.result()?.cartChanged,true);
});

for(const [label,damage] of [['product identity',p=>p.id='gid://shopify/Product/999'],['exact option',p=>p.variants.at(-1).options[0].value='Sterling Silver'],['held product',p=>p.cartHold=true],['incomplete variants',p=>p.variantsComplete=false]])test('production voice add rejects fresh '+label+' mismatch without a cart post',async t=>{
  const h=await productionFixture(t),changed=checkedPiece(h.raw);damage(changed);h.api(changed);await h.say('Add this piece to my cart');assert.equal(h.hookCalls.length,1);assert.equal(h.postCount(),0);assert.equal(h.cart().item_count,0);assert.ok(h.result()?.error,JSON.stringify(h.result()));assert.notEqual(h.result()?.ok,true);assert.notEqual(h.result()?.cartChanged,true);
});

for(const reason of ['new speech','native options','page navigation','hidden page'])test('a production add paused on fresh product read cannot write after '+reason,async t=>{
  const h=await productionFixture(t),gate=h.deferProduct(),input=h.begin();h.commit(input);h.final(input,'Add this piece to my cart');await settle();assert.equal(h.hookCalls.length,1);assert.equal(h.postCount(),0);
  if(reason==='new speech')h.emit({type:'input_audio_buffer.speech_started',item_id:'shopping43-new-input'});if(reason==='native options')await h.changeMetal('Sterling Silver');if(reason==='page navigation')await h.moveToCart();if(reason==='hidden page')h.hide();gate.resolve();await settle();assert.equal(h.postCount(),0);assert.equal(h.cart().item_count,0);assert.equal(h.receipts().some(e=>e.item.content[0].text.includes('"cartChanged":true')),false);
});

test('an uncertain production cart readback blocks a fresh spoken retry for that variant',async t=>{
  const h=await productionFixture(t);h.uncertain();await h.say('Add this piece to my cart');assert.equal(h.postCount(),1);assert.ok(h.result()?.error,JSON.stringify(h.result()));assert.notEqual(h.result()?.ok,true);assert.equal(h.cart().item_count,0);assert.deepEqual(JSON.parse(h.w.sessionStorage.getItem('brites-concierge-v1')).uncertainVariants,['4304']);await h.say('Add this piece to my cart');assert.equal(h.postCount(),1);assert.ok(h.result()?.error);assert.notEqual(h.result()?.ok,true);assert.equal(h.cart().item_count,0);
});

test('interruption after server acceptance records uncertainty and cannot duplicate the cart on a fresh voice retry',async t=>{
  const h=await productionFixture(t),gate=h.deferAdd({acceptAborted:true});await h.say('Add this piece to my cart');assert.equal(h.postCount(),1);assert.equal(h.cart().item_count,0);h.emit({type:'input_audio_buffer.speech_started',item_id:'new-input-after-post'});gate.resolve();await settle();assert.equal(h.cart().item_count,3);assert.equal(h.receipts().length,0);assert.deepEqual(JSON.parse(h.w.sessionStorage.getItem('brites-concierge-v1')).uncertainVariants,['4304']);await h.say('Add this piece to my cart');assert.equal(h.postCount(),1);assert.equal(h.cart().item_count,3);assert.ok(h.result()?.error);assert.notEqual(h.result()?.cartChanged,true);
});

test('failed/old native commit and stale model response cannot revive an earlier spoken add request',async t=>{
  const h=await productionFixture(t),old=h.begin();h.final(old,'Add this piece to my cart');await settle();assert.equal(h.postCount(),0);assert.equal(h.controls.length,0);assert.equal(h.responses().length,0);
  await h.say('How big is it?');assert.equal(h.result().productFacts.handle,HANDLE);const before=h.controls.length;h.commit(old);h.final(old,'Add this piece to my cart');h.emit({type:'response.created',response:{id:'unissued-old-response',metadata:{brites_voice_request:'guessed-old-request',brites_input_item:old.itemId,brites_turn_version:String(old.version)}}});h.emit({type:'response.function_call_arguments.done',name:'control_storefront',response_id:'unissued-old-response',call_id:'old-model-add',arguments:JSON.stringify({type:'add',handle:HANDLE,variantId:VARIANT})});await settle();assert.equal(h.postCount(),0);assert.equal(h.controls.length,before);assert.equal(h.receipts().length,1);
});

test('model add arguments on a finalized information question cannot substitute for shopper cart intent',async t=>{
  const h=await productionFixture(t),input=await h.say('How big is it?'),response=h.responses().at(-1);h.emit({type:'response.created',response:{id:'fact-only-model-response',metadata:response.response.metadata}});h.emit({type:'response.function_call_arguments.done',name:'control_storefront',response_id:'fact-only-model-response',call_id:'information-model-add',arguments:JSON.stringify({type:'add',handle:HANDLE,variantId:VARIANT})});await settle();assert.equal(h.postCount(),0);assert.equal(h.hookCalls.length,0);assert.equal(h.controls.length,0);assert.equal(h.receipts().length,1);
  const hostile=await h.config.onTool({type:'add',handle:'another-piece',variantId:'gid://shopify/ProductVariant/999'},{name:'control_storefront',inputItemId:input.itemId,turnVersion:input.version,currentTurn:true,responseId:'hostile-direct-call'});assert.ok(hostile.error);assert.equal(h.postCount(),0);assert.equal(h.controls.length,0);
});

for(const drift of ['price','currency','availability'])test('native live '+drift+' drift is rejected before production add authority can write',async t=>{
  const h=await productionFixture(t);if(drift==='price')h.raw.variants.at(-1).price+=100;if(drift==='currency')h.setCartCurrency('CAD');if(drift==='availability')h.raw.variants.at(-1).available=false;await h.say('Add this piece to my cart');assert.equal(h.postCount(),0);assert.equal(h.hookCalls.length,0);assert.ok(h.result()?.error);assert.equal(h.cart().item_count,0);
});

test('missing production choices keep exact native context and guide the unchosen metal without posting',async t=>{
  const h=await productionFixture(t,{selected:false});const before=Voice.publicContext(h.config.getContext());assert.equal(before.productControls.selectionStatus,'choosing');assert.deepEqual(before.productControls.requiredOptions,['Metal Choice']);assert.equal(before.productControls.selectedVariant,undefined);
  await h.say('Add this piece to my cart');assert.equal(h.postCount(),0);assert.equal(h.hookCalls.length,0);assert.deepEqual(clone(h.store.snapshot().productControls.selectedOptions),[{name:'Metal Choice',value:''},{name:'Chain Length',value:'18 inches'}]);const next=Voice.publicContext(h.config.getContext());assert.equal(next.productControls.openedOption,'Metal Choice');assert.equal(next.productControls.optionsOpen,true);assert.equal(next.productControls.selectionStatus,'choosing');assert.deepEqual(next.productControls.requiredOptions,['Metal Choice']);assert.match(h.result()?.reply||h.result()?.error||'',/Sterling Silver.*Gold Filled/);assert.notEqual(h.result()?.cartChanged,true);assert.ok(h.scrolled.includes(h.d.querySelector('#bjMetals')));
});

test('missing production length opens the actual native dropdown and a spoken literal makes the exact choice ready',async t=>{
  const h=await productionFixture(t,{missingLength:true}),select=h.d.querySelector('select.bjOptSel'),button=h.d.querySelector('.bjselx__btn');assert.equal(select.value,'');await h.say('Add this piece to my cart');assert.equal(button.getAttribute('aria-expanded'),'true');assert.equal(select.value,'');assert.equal(h.postCount(),0);assert.equal(h.hookCalls.length,0);assert.equal(Voice.publicContext(h.config.getContext()).productControls.openedOption,'Chain Length');assert.match(h.result()?.reply||h.result()?.error||'',/16 inches.*18 inches/);
  await h.say('Select 18 inches');assert.equal(select.value,'18 inches');assert.equal(h.result()?.ok,true);const ready=Voice.publicContext(h.config.getContext()).productControls;assert.equal(ready.selectionStatus,'ready');assert.equal(ready.selectedVariant.id,VARIANT);assert.equal(ready.quantity,3);assert.equal(ready.itemTotalPrice,225);assert.equal(h.postCount(),0);
  await h.say('Add this piece to my cart');assert.equal(h.postCount(),1);assert.equal(h.cart().item_count,3);assert.equal(h.result()?.cartChanged,true);
});

test('unread production bag count stays explicitly unknown instead of claiming an empty bag',async t=>{
  const h=await productionFixture(t,{refresh:false,voice:false});const raw=h.store.snapshot(),bridge=Bridge.sanitizeSnapshot(raw),native=Voice.publicContext(bridge);assert.equal(raw.bagControls.countKnown,false);assert.equal(bridge.bagControls.countKnown,false);assert.equal(native.bagControls.countKnown,false);assert.equal(native.bagControls.linesComplete,false);
  assert.equal(await h.store.refresh(),true);const checked=Voice.publicContext(Bridge.sanitizeSnapshot(h.store.snapshot()));assert.equal(checked.bagControls.countKnown,true);assert.equal(checked.bagControls.itemCount,0);assert.equal(checked.bagControls.linesComplete,true);
});
