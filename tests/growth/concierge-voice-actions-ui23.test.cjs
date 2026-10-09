'use strict';
// Exercise the actual widget with deterministic native-adapter callbacks and
// synthetic shop data. This does not certify microphone, WebRTC or GPU quality.
const test=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const {JSDOM,VirtualConsole}=require('jsdom');
const widget=fs.readFileSync(require.resolve('../../brites-concierge.js'),'utf8');
const actions=fs.readFileSync(require.resolve('../../brites-concierge-voice-actions.js'),'utf8');
// Observe the real host outcome immediately before the shopper projection.
// Only this evaluated fixture is instrumented; no authority, input or result
// is replaced. Keying observations by the returned packet preserves concurrent
// preparation and cancellation checks without exposing internals to speech.
const receiptStart=widget.indexOf('  function nativeControlReceipt(result){'),receiptEnd=widget.indexOf('\n  async function replayVoiceFastResult',receiptStart);
assert.ok(receiptStart>=0&&receiptEnd>receiptStart,'the shopper projection must remain an explicit observation boundary');
const receiptSource=widget.slice(receiptStart,receiptEnd),receiptReturn='    return out;';
assert.equal(receiptSource.split(receiptReturn).length,2,'capture the one real shopper receipt return');
const observedWidget=widget.slice(0,receiptStart)+receiptSource.replace(receiptReturn,'    window.BritesVoiceReceiptFixtureObserver(out,result);\n'+receiptReturn)+widget.slice(receiptEnd);
const clone=value=>JSON.parse(JSON.stringify(value));
const tick=()=>new Promise(resolve=>setImmediate(resolve));
async function settle(){await tick();await tick();}
function deferred(){let resolve;const promise=new Promise(r=>{resolve=r;});return {promise,resolve};}
const silver16={id:'gid://shopify/ProductVariant/101',numericId:'101',title:'Sterling Silver / 16 inch / None',price:54,available:true,options:[{name:'Metal',value:'Sterling Silver'},{name:'Length',value:'16 inch'},{name:'Engraving',value:'None'}]};
const silver18={...clone(silver16),id:'gid://shopify/ProductVariant/102',numericId:'102',title:'Sterling Silver / 18 inch / None',options:[{name:'Metal',value:'Sterling Silver'},{name:'Length',value:'18 inch'},{name:'Engraving',value:'None'}]};
const engraved={...clone(silver16),id:'gid://shopify/ProductVariant/103',numericId:'103',title:'Sterling Silver / 16 inch / Engraved',price:64,options:[{name:'Metal',value:'Sterling Silver'},{name:'Length',value:'16 inch'},{name:'Engraving',value:'Engraved'}]};
const piece={id:'gid://shopify/Product/1',handle:'compass-necklace',url:'https://britesjewelry.com/products/compass-necklace',title:'Compass Necklace',type:'Necklace',currency:'USD',variants:[silver16,silver18,engraved],variantsComplete:true,minPrice:54,suggestedVariantId:silver16.id,why:'A personal reminder of your milestone.',checkedAt:1790992501430};
const other={...clone(piece),id:'gid://shopify/Product/2',handle:'bunny-necklace',url:'https://britesjewelry.com/products/bunny-necklace',title:'Bunny Necklace',variants:[{...clone(silver16),id:'gid://shopify/ProductVariant/201',numericId:'201'}]};
const exactReview='Review adding the Compass Necklace with sterling silver, sixteen inch length and no engraving to my bag.';
function assertShopperReceipt(packet,actual){
  assert.equal(typeof packet.reply,'string');assert.ok(packet.reply.trim());assert.equal(packet.customerMessage,packet.reply);
  assert.equal(packet.ok,actual?.ok===true||actual?.prepared===true&&actual?.ok!==false&&!actual?.error,'spoken success must match the actual host outcome');
  assert.equal(packet.cartChanged,actual?.cartChanged===true);
  for(const key of ['error','reason','action','actions','completedActions','stepResults','product','variant','variantId','snapshot','publicContext','authority','requestId','responseId','turnVersion'])assert.equal(Object.hasOwn(packet,key),false,key+' belongs outside the shopper receipt');
  assert.doesNotMatch(packet.reply,/\b(?:backend|adapter|postcondition|precondition|metadata|authority|control_storefront|NO_ACTION_AUTHORITY|EXACT_OPTIONS_REQUIRED)\b|\bmotif\s*[:=]|\bno further actions?\b/i);
  assert.doesNotMatch(JSON.stringify(packet),/PRIVATE_|privateNotes|giftNote|properties/);
}
function fixture(t,options={}){
  const errors=[],virtualConsole=new VirtualConsole();virtualConsole.on('jsdomError',e=>errors.push(e));
  const dom=new JSDOM('<!doctype html><html><body></body></html>',{url:'https://growth-sandbox.example'+(options.path||'/concierge-sandbox.html'),runScripts:'outside-only',pretendToBeVisual:true,virtualConsole});
  const w=dom.window,d=w.document,script=d.createElement('script');script.src='https://growth-sandbox.example/brites-concierge.js';script.dataset.sandbox=options.sandbox===false?'false':'true';if(options.fixtureFlag)script.dataset.voiceFixture='true';Object.defineProperty(d,'currentScript',{get:()=>script});
  let hidden=false,config,turnVersion=0,callSerial=0;Object.defineProperty(d,'hidden',{get:()=>hidden});
  const calls={start:0,stop:0,nativeFactory:0,fixtureFactory:0,requests:[],productReads:[],timers:[],nativeReceipts:[]},nativeOutcomes=new WeakMap();
  w.BritesVoiceReceiptFixtureObserver=(packet,actual)=>nativeOutcomes.set(packet,actual);
  const client={start:async()=>{calls.start++;return true;},stop:async()=>{calls.stop++;},interrupt(){},dispose:async()=>{}};
  w.BritesConciergeVoice={create(value){calls.nativeFactory++;config=value;return client;}};
  w.BritesConciergeVoiceActionTestAdapter={create(value){calls.fixtureFactory++;config=value;return client;}};
  w.BritesConciergeAvatar={create(){return {setState(){},setVisible(){},retry(){},setEmotion(){},setPaused(){},setLevel(){},triggerGreeting(){}};}};
  if(options.helper!==false)w.eval(actions);
  if(options.saved)w.sessionStorage.setItem('brites-concierge-v1',JSON.stringify(options.saved));
  const products=clone(options.products||[piece]);let currentAnswer=options.answer||null,live=clone(piece),market={currency:options.storefrontCurrency||'CAD',variants:piece.variants.map(v=>({id:Number(v.numericId),price:(v.price+20)*100,available:v.available}))};
  if(options.sandbox===false)w.Shopify={currency:{active:market.currency},routes:{root:'/en-ca/'}};
  if(options.timers){const set=w.setTimeout.bind(w),clear=w.clearTimeout.bind(w);w.setTimeout=(fn,ms)=>{if(ms===9000){const id=100000+calls.timers.length;calls.timers.push({fn,ms,id});return id;}return set(fn,ms);};w.clearTimeout=id=>{if(id<100000)clear(id);};}
  w.fetch=async(url,init={})=>{
    const body=init.body?JSON.parse(init.body):{},entry={url:String(url),body,signal:init.signal};calls.requests.push(entry);
    if(String(url).endsWith('/cart.js')){if(options.marketRead)await options.marketRead(entry);return {ok:market.ok!==false,json:async()=>({currency:market.currency,items:[]})};}
    if(String(url).includes('/products/')&&String(url).endsWith('.js'))return {ok:market.ok!==false,json:async()=>({variants:clone(market.variants)})};
    if(String(url).includes('/api/growth/product?')){calls.productReads.push(entry);const payload=options.readProduct?await options.readProduct(entry):{product:clone(live),live:true};return {ok:payload.ok!==false,json:async()=>payload};}
    return {ok:true,json:async()=>body.message?(currentAnswer?clone(currentAnswer):{live:true,reply:'Checked catalogue summary.',preferences:{},products:clone(products),meanings:clone(options.meanings||[])}):{}};
  };
  w.HTMLElement.prototype.scrollIntoView=function(){};w.eval(observedWidget);
  const root=d.querySelector('brites-concierge').shadowRoot,button=label=>[...root.querySelectorAll('button')].find(n=>n.textContent.trim()===label||n.getAttribute('aria-label')===label);
  const tool=async(name,args,context={})=>{const packet=await config.onTool(args,{name,callId:'synthetic-call-'+(++callSerial),...context});if(nativeOutcomes.has(packet)){const actual=nativeOutcomes.get(packet);assertShopperReceipt(packet,actual);calls.nativeReceipts.push({packet:clone(packet),actual:clone(actual)});return actual;}return packet;};
  async function activate(){button('Talk to me').click();await settle();root.querySelector('script[src$="brites-concierge-voice.js"]')?.dispatchEvent(new w.Event('load'));await settle();}
  async function display(){await tool('find_jewellery',{message:'A necklace for graduation'});}
  function speak(text,onlyStart=false){const itemId='synthetic-input-'+(++turnVersion);config.onSpeechStarted({itemId,turnVersion,reason:'speech'});if(!onlyStart)config.onTranscript({role:'user',itemId,turnVersion,currentTurn:true,text,final:true});return {itemId,inputItemId:itemId,turnVersion,currentTurn:true};}
  async function ready(){w.BritesConcierge.open();await activate();await display();}
  t.after(()=>{try{w.BritesConcierge.close();}catch{}w.close();});
  return {w,d,root,button,calls,errors,tool,activate,display,speak,ready,get config(){return config;},setLive(value){live=clone(value);},setAnswer(value){currentAnswer=clone(value);},setMarket(value){market={...market,...clone(value)};},saved:()=>JSON.parse(w.sessionStorage.getItem('brites-concierge-v1')),hide(value){hidden=value;d.dispatchEvent(new w.Event('visibilitychange'));}};
}

test('named search returns exact identities and sanitized meanings; inspect is read only and reports current type, options and minimum price',async t=>{
  const meanings=[{productId:piece.id,text:'A personal milestone interpretation.',context:'graduation',sources:[{title:'Neutral source',url:'https://museum.example/symbol'},{title:'Unsafe',url:'javascript:alert(1)'}]},{productId:'gid://shopify/Product/999',text:'WRONG PIECE',sources:[]}];
  const h=fixture(t,{meanings});await h.ready();h.config.onTranscript({role:'assistant',itemId:'synthetic-output',text:'Let’s choose together.',final:true});const card=h.root.querySelector('.card'),caption=h.root.querySelector('.caption-text').textContent,history=JSON.stringify(h.saved().history);
  h.setLive({...clone(piece),type:'Pendant necklace',variants:[{...clone(silver16),price:59},silver18]});
  const checked=await h.tool('inspect_jewellery',{handle:piece.handle});assert.equal(checked.product.id,piece.id);assert.equal(checked.product.type,'Pendant necklace');assert.equal(checked.product.minPrice,54);assert.equal(checked.product.variants[0].price,59);assert.equal(checked.product.variants[0].id,silver16.id);assert.equal(checked.product.variants[0].options[1].value,'16 inch');assert.equal(checked.product.currency,'USD');assert.equal(checked.live,true);assert.deepEqual([...checked.actions],[]);assert.equal(h.root.querySelector('.card'),card);assert.equal(h.root.querySelector('.caption-text').textContent,caption);assert.equal(JSON.stringify(h.saved().history),history);assert.equal(h.w.sessionStorage.getItem('brites-sandbox-cart'),null);
  const result=await h.tool('find_jewellery',{message:'A necklace for graduation'});assert.equal(result.products[0].id,piece.id);assert.equal(result.products[0].handle,piece.handle);assert.equal(result.meanings.length,1);assert.equal(result.meanings[0].sources.length,1);assert.doesNotMatch(JSON.stringify(result),/javascript:|WRONG PIECE/);
});
for(const [name,boundary] of [
  ['unavailable policy',{policyOnly:true,policyUnavailable:true,policyKnowledge:{status:'partial'}}],
  ['checkout boundary',{checkoutBoundary:true}],
  ['shared budget clarification',{budgetClarification:true}]
])test(name+' preserves visible cards but returns no old product prices or meanings as current evidence',async t=>{
  const h=fixture(t,{meanings:[{productId:piece.id,text:'OLD STORED INTERPRETATION',context:'graduation',sources:[]}]});await h.ready();const card=h.root.querySelector('.card');h.setAnswer({live:false,...boundary,reply:'Current guidance does not include a fresh product check.',preferences:{},products:[],meanings:[]});const result=await h.tool('find_jewellery',{message:'Current policy or checkout question'});assert.equal(result.verified,false);assert.equal(result.products.length,0);assert.equal(result.meanings.length,0);assert.equal(h.root.querySelector('.card'),card);assert.equal(h.saved().products[0].minPrice,54);assert.equal(h.saved().meanings[0].text,'OLD STORED INTERPRETATION');assert.equal(result.displayedPieces.length,1);assert.deepEqual(Object.keys(result.displayedPieces[0]).sort(),['handle','id','title']);assert.doesNotMatch(JSON.stringify(result),/OLD STORED INTERPRETATION|minPrice|currency|https:\/\/britesjewelry\.com/);
});
test('verified productless policy guidance remains separate from preserved product facts',async t=>{
  const h=fixture(t);await h.ready();h.setAnswer({live:true,policyOnly:true,policyKnowledge:{status:'verified'},reply:'The current published policy was checked.',preferences:{},products:[],meanings:[]});const result=await h.tool('find_jewellery',{message:'Explain the current shipping policy'});assert.equal(result.verified,true);assert.equal(result.products.length,0);assert.equal(result.meanings.length,0);assert.equal(result.displayedPieces[0].id,piece.id);assert.equal(h.root.querySelectorAll('.card').length,1);
});
test('a policy-only response with partial knowledge cannot label itself verified even when its live flag is true',async t=>{
  const h=fixture(t);await h.ready();h.setAnswer({live:true,policyOnly:true,policyKnowledge:{status:'partial'},reply:'Some policy details could not be checked.',preferences:{},products:[],meanings:[]});const result=await h.tool('find_jewellery',{message:'Explain the current shipping policy'});assert.equal(result.verified,false);assert.equal(result.products.length,0);assert.equal(result.meanings.length,0);
});
for(const flag of [false,undefined])test('a '+String(flag)+' live flag cannot certify response products or meanings',async t=>{
  const h=fixture(t);await h.ready();h.setAnswer({live:flag,reply:'An unverified catalogue response.',preferences:{},products:[clone(piece)],meanings:[{productId:piece.id,text:'UNVERIFIED INTERPRETATION',context:'graduation',sources:[]}]});const result=await h.tool('find_jewellery',{message:'Try the search again'});assert.equal(result.verified,false);assert.equal(result.products.length,0);assert.equal(result.meanings.length,0);assert.deepEqual(Object.keys(result.displayedPieces[0]).sort(),['handle','id','title']);assert.doesNotMatch(JSON.stringify(result),/UNVERIFIED INTERPRETATION|minPrice|currency/);
});
test('fresh live search returns only the actual newly checked response identities and their sanitized meanings',async t=>{
  const h=fixture(t,{meanings:[{productId:piece.id,text:'OLD STORED INTERPRETATION',context:'graduation',sources:[]}]});await h.ready();const fresh={...clone(other),minPrice:83,variants:other.variants.map(v=>({...clone(v),price:83}))};h.setAnswer({live:true,reply:'A new piece was checked.',preferences:{},products:[fresh],meanings:[{productId:other.id,text:'A current personal interpretation.',context:'graduation',sources:[{title:'Neutral source',url:'https://museum.example/meaning'}]},{productId:piece.id,text:'STALE PRODUCT MEANING',context:'graduation',sources:[]}]});const result=await h.tool('find_jewellery',{message:'Find a different current piece'});assert.equal(result.verified,true);assert.equal(result.products.length,1);assert.equal(result.products[0].id,other.id);assert.equal(result.products[0].minPrice,83);assert.equal(result.meanings.length,1);assert.equal(result.meanings[0].productId,other.id);assert.equal(result.displayedPieces.length,0);assert.doesNotMatch(JSON.stringify(result),/OLD STORED INTERPRETATION|STALE PRODUCT MEANING/);
});
test('inspect caps spoken variants at 24 without claiming an exhaustive complete option list',async t=>{
  const h=fixture(t);await h.ready();h.setLive({...clone(piece),variants:Array.from({length:25},(_,i)=>({...clone(silver16),id:'gid://shopify/ProductVariant/'+(300+i),numericId:String(300+i),title:'Choice '+(i+1)}))});const checked=await h.tool('inspect_jewellery',{handle:piece.handle});assert.equal(checked.product.variants.length,24);assert.equal(checked.product.optionsTruncated,true);assert.equal(checked.product.variantsComplete,false);assert.equal(h.root.querySelectorAll('.card').length,1);
});
test('large exact option labels are bounded by serialized byte size and are explicitly partial',async t=>{
  const h=fixture(t);await h.ready();h.setLive({...clone(piece),variants:Array.from({length:24},(_,i)=>({...clone(silver16),id:'gid://shopify/ProductVariant/'+(400+i),numericId:String(400+i),title:'Choice '+(i+1),options:Array.from({length:12},(_,j)=>({name:'Choice '+j,value:'珠'.repeat(300)}))}))});const result=await h.tool('inspect_jewellery',{handle:piece.handle});assert.ok(Buffer.byteLength(JSON.stringify(result),'utf8')<30000);assert.ok(result.product.variants.length<24);assert.equal(result.product.optionsTruncated,true);assert.equal(result.product.variantsComplete,false);
});
test('inspection refuses an undisplayed target, invalid handle and missing helper without substituting a first result',async t=>{
  const h=fixture(t);await h.ready();for(const handle of ['not-shown','../compass-necklace','',null])assert.ok((await h.tool('inspect_jewellery',{handle})).error);assert.equal(h.calls.productReads.length,0);delete h.w.BritesConciergeVoiceActions;assert.ok((await h.tool('inspect_jewellery',{handle:piece.handle})).error);assert.equal(h.calls.productReads.length,0);
});
for(const [name,mutate] of [
  ['not-live',()=>({product:clone(piece),live:false})],
  ['identity',()=>({product:{...clone(piece),id:other.id},live:true})],
  ['handle',()=>({product:{...clone(piece),handle:other.handle},live:true})],
  ['foreign URL',()=>({product:{...clone(piece),url:'https://competitor.example/products/compass-necklace'},live:true})],
  ['wrong owned URL',()=>({product:{...clone(piece),url:other.url},live:true})],
  ['cart hold',()=>({product:{...clone(piece),cartHold:true},live:true})],
  ['recommendation hold',()=>({product:{...clone(piece),recommendationHold:true},live:true})],
  ['parts only',()=>({product:{...clone(piece),partsOnly:true},live:true})],
  ['unavailable options',()=>({product:{...clone(piece),variants:piece.variants.map(v=>({...clone(v),available:false}))},live:true})],
  ['duplicate variant identity',()=>({product:{...clone(piece),variants:[clone(silver16),clone(silver16)]},live:true})]
])test('live '+name+' refuses inspection and preparation without changing the displayed selection',async t=>{
  const h=fixture(t,{readProduct:mutate});await h.ready();const card=h.root.querySelector('.card');assert.ok((await h.tool('inspect_jewellery',{handle:piece.handle})).error);const context=h.speak(exactReview);assert.ok((await h.tool('prepare_jewellery_action',{handle:piece.handle,action:'review',variantId:silver16.id},context)).error);assert.equal(h.root.querySelector('.card'),card);assert.equal(h.root.querySelector('.review'),null);assert.equal(h.w.sessionStorage.getItem('brites-sandbox-cart'),null);
});

test('explicit spoken view requests owned-page navigation and preserves a fallback Open button without adding to bag',async t=>{
  const h=fixture(t);await h.ready();const href=h.w.location.href,stops=h.calls.stop,context=h.speak('Open the Compass Necklace page.');const result=await h.tool('prepare_jewellery_action',{handle:piece.handle,action:'view'},context);assert.equal(result.prepared,true);assert.equal(result.cartChanged,false);assert.equal(result.requiredCustomerClick,null);assert.equal(result.navigationRequested,true);assert.ok(h.button('Open this piece'));assert.equal(h.root.activeElement,h.button('Open this piece'));assert.equal(h.w.location.href,href);assert.equal(h.calls.stop,stops);assert.ok(h.button('End voice'));assert.equal(h.w.sessionStorage.getItem('brites-sandbox-cart'),null);assert.ok((await h.tool('prepare_jewellery_action',{handle:piece.handle,action:'view'},context)).error);
});
test('spoken options opens the actual current choices while keeping native voice and optional captions independent',async t=>{
  const h=fixture(t);await h.ready();const context=h.speak('Show the options for the Compass Necklace.');const result=await h.tool('prepare_jewellery_action',{handle:piece.handle,action:'options'},context);assert.equal(result.prepared,true);assert.equal(h.root.querySelectorAll('select option').length,3);assert.equal(h.root.activeElement,h.root.querySelector('select'));assert.equal(h.root.querySelector('.captions').hidden,true);assert.equal(h.root.querySelector('.composer').hidden,true);assert.ok(h.button('End voice'));assert.equal(h.root.querySelector('.review'),null);assert.equal(h.w.sessionStorage.getItem('brites-sandbox-cart'),null);
});
test('spoken exact review preselects the transcript-evidenced variant and requires the separate actual Confirm click',async t=>{
  const h=fixture(t);await h.ready();const context=h.speak(exactReview),stops=h.calls.stop;const result=await h.tool('prepare_jewellery_action',{handle:piece.handle,action:'review',variantId:silver16.id},context);assert.equal(result.prepared,true);assert.equal(result.variantId,silver16.id);assert.equal(result.cartChanged,false);assert.equal(h.root.querySelector('select').value,silver16.id);assert.match(h.root.querySelector('.review').textContent,/Sterling Silver \/ 16 inch \/ None/);assert.ok(h.button('Confirm add to bag'));assert.equal(h.calls.stop,stops);assert.ok(h.button('End voice'));assert.equal(h.w.sessionStorage.getItem('brites-sandbox-cart'),null);assert.equal(h.calls.requests.some(r=>r.url.includes('/cart/add.js')),false);
  h.button('Confirm add to bag').click();await settle();const cart=JSON.parse(h.w.sessionStorage.getItem('brites-sandbox-cart'));assert.equal(cart.length,1);assert.equal(cart[0].variantId,'101');assert.equal(h.calls.productReads.length,2,'the explicit confirmation performs another current live check');assert.equal(h.calls.requests.some(r=>r.url.includes('/cart/add.js')),false);
});

for(const [name,text,args,metadata] of [
  ['model-invented request','I like the Compass Necklace.',{handle:piece.handle,action:'review',variantId:silver16.id,message:exactReview},{}],
  ['ambiguous variant','Add the Compass Necklace to my bag.',{handle:piece.handle,action:'review',variantId:silver16.id},{}],
  ['different exact variant',exactReview,{handle:piece.handle,action:'review',variantId:silver18.id},{}],
  ['personalization','Add the Compass Necklace sterling silver sixteen inch engraved to my bag.',{handle:piece.handle,action:'review',variantId:engraved.id},{}],
  ['negation','Do not open the Compass Necklace.',{handle:piece.handle,action:'view'},{}],
  ['conditional','If I choose the Compass Necklace, open it later.',{handle:piece.handle,action:'view'},{}],
  ['wrong input item',exactReview,{handle:piece.handle,action:'review',variantId:silver16.id},{inputItemId:'different-input'}],
  ['wrong turn version',exactReview,{handle:piece.handle,action:'review',variantId:silver16.id},{turnVersion:999}],
  ['noncurrent response',exactReview,{handle:piece.handle,action:'review',variantId:silver16.id},{currentTurn:false}]
])test(name+' cannot prepare a product action',async t=>{
  const h=fixture(t);await h.ready();const context={...h.speak(text),...metadata},card=h.root.querySelector('.card');const result=await h.tool('prepare_jewellery_action',args,context);assert.ok(result.error);assert.equal(h.root.querySelector('.card'),card);assert.equal(h.root.querySelector('.review'),null);assert.equal(h.button('Open this piece'),undefined);assert.equal(h.w.sessionStorage.getItem('brites-sandbox-cart'),null);
});
test('a pronoun across multiple pieces cannot silently select the first; a shopper-selected piece provides exact scope',async t=>{
  const h=fixture(t,{products:[piece,other]});await h.ready();let context=h.speak('Open it.');assert.ok((await h.tool('prepare_jewellery_action',{handle:piece.handle,action:'view'},context)).error);h.root.querySelectorAll('.choose')[1].click();h.setLive(other);context=h.speak('Open it.');const result=await h.tool('prepare_jewellery_action',{handle:other.handle,action:'view'},context);assert.equal(result.product.id,other.id); // fresh live target must match the selected product
});

for(const boundary of ['speech','interrupt','End voice','hidden','close','fresh','selection','search'])test(boundary+' expires a prepared review and removes its stale Confirm gate',async t=>{
  const h=fixture(t);await h.ready();const context=h.speak(exactReview);assert.equal((await h.tool('prepare_jewellery_action',{handle:piece.handle,action:'review',variantId:silver16.id},context)).prepared,true);
  if(boundary==='speech')h.speak('',true);
  else if(boundary==='interrupt'){h.config.onState('speaking');h.button('Let me speak').click();}
  else if(boundary==='End voice')h.button('End voice').click();
  else if(boundary==='hidden')h.hide(true);
  else if(boundary==='close')h.w.BritesConcierge.close();
  else if(boundary==='fresh'){h.button('Type instead').click();h.button('Start fresh').click();}
  else if(boundary==='selection'){const select=h.root.querySelector('select');select.value=silver18.id;select.dispatchEvent(new h.w.Event('change',{bubbles:true}));}
  else await h.display();
  assert.equal(h.root.querySelector('.review'),null);assert.equal(h.button('Confirm add to bag'),undefined);assert.equal(h.w.sessionStorage.getItem('brites-sandbox-cart'),null);assert.ok((await h.tool('prepare_jewellery_action',{handle:piece.handle,action:'review',variantId:silver16.id},context)).error);
});
test('new speech removes a prepared Open button and late old final audio cannot restore authority',async t=>{
  const h=fixture(t);await h.ready();const old=h.speak('Open the Compass Necklace.');await h.tool('prepare_jewellery_action',{handle:piece.handle,action:'view'},old);h.speak('',true);assert.equal(h.button('Open this piece'),undefined);h.config.onTranscript({role:'user',itemId:old.inputItemId,turnVersion:old.turnVersion,currentTurn:false,text:'Open the Compass Necklace.',final:true});assert.ok((await h.tool('prepare_jewellery_action',{handle:piece.handle,action:'view'},old)).error);
});
test('concurrent prepare calls consume at most one actual shopper turn',async t=>{
  const pending=deferred(),h=fixture(t,{readProduct:()=>pending.promise});await h.ready();const context=h.speak(exactReview),first=h.tool('prepare_jewellery_action',{handle:piece.handle,action:'review',variantId:silver16.id},context);await settle();const second=await h.tool('prepare_jewellery_action',{handle:piece.handle,action:'review',variantId:silver16.id},context);assert.ok(second.error);assert.equal(h.calls.productReads.length,1);pending.resolve({live:true,product:clone(piece)});assert.equal((await first).prepared,true);assert.equal(h.root.querySelectorAll('.review').length,1);assert.equal(h.w.sessionStorage.getItem('brites-sandbox-cart'),null);
});
for(const boundary of ['signal','speech','End voice','hidden','selection','search'])test(boundary+' cancels an in-flight read even if fetch ignores abort',async t=>{
  const pending=deferred(),h=fixture(t,{readProduct:()=>pending.promise});await h.ready();const context=h.speak(exactReview),abort=new h.w.AbortController(),before=h.root.querySelector('.card'),prepare=h.tool('prepare_jewellery_action',{handle:piece.handle,action:'review',variantId:silver16.id},{...context,signal:abort.signal});await settle();
  if(boundary==='signal')abort.abort();else if(boundary==='speech')h.speak('',true);else if(boundary==='End voice')h.button('End voice').click();else if(boundary==='hidden')h.hide(true);else if(boundary==='selection')h.button('Choose options').click();else await h.display();
  assert.equal(h.calls.productReads[0].signal.aborted,true);assert.match((await prepare).error,/cancelled|could not be checked/);pending.resolve({live:true,product:clone(piece)});await settle();assert.equal(h.root.querySelector('.review'),null);assert.equal(h.button('Confirm add to bag'),undefined);if(boundary!=='search')assert.equal(h.root.querySelector('.card'),before);assert.equal(h.w.sessionStorage.getItem('brites-sandbox-cart'),null);
});
test('bounded live inspection releases a fetch that never responds and cannot install a late review',async t=>{
  const pending=deferred(),h=fixture(t,{timers:true,readProduct:()=>pending.promise});await h.ready();const context=h.speak(exactReview),prepare=h.tool('prepare_jewellery_action',{handle:piece.handle,action:'review',variantId:silver16.id},context);await settle();assert.equal(h.calls.timers.length,1);assert.equal(h.calls.timers[0].ms,9000);h.calls.timers[0].fn();assert.match((await prepare).error,/cancelled/);pending.resolve({live:true,product:clone(piece)});await settle();assert.equal(h.root.querySelector('.review'),null);
});
test('saved spoken history cannot restore action authority after reload or native voice restart',async t=>{
  const first=fixture(t);await first.ready();const context=first.speak(exactReview),saved=first.saved();const h=fixture(t,{saved});h.w.BritesConcierge.open();await h.activate();assert.equal(h.root.querySelectorAll('.card').length,1);assert.ok((await h.tool('prepare_jewellery_action',{handle:piece.handle,action:'review',variantId:silver16.id},context)).error);const fresh=h.speak(exactReview);await h.tool('prepare_jewellery_action',{handle:piece.handle,action:'review',variantId:silver16.id},fresh);h.button('End voice').click();await h.activate();assert.equal(h.root.querySelector('.review'),null);assert.ok((await h.tool('prepare_jewellery_action',{handle:piece.handle,action:'review',variantId:silver16.id},fresh)).error);
});
test('on-demand helper loading is single-flight, does not block native start, and a product tool waits for its bounded result',async t=>{
  const h=fixture(t,{helper:false});await h.ready();assert.ok(h.button('End voice'));let loaders=h.root.querySelectorAll('script[src$="brites-concierge-voice-actions.js"]');assert.equal(loaders.length,1);const pending=h.tool('inspect_jewellery',{handle:piece.handle});await settle();assert.equal(h.calls.productReads.length,0);h.button('End voice').click();await h.activate();loaders=h.root.querySelectorAll('script[src$="brites-concierge-voice-actions.js"]');assert.equal(loaders.length,1);assert.match((await pending).error,/cancelled/);
  const current=h.tool('inspect_jewellery',{handle:piece.handle});h.w.eval(actions);loaders[0].dispatchEvent(new h.w.Event('load'));assert.equal((await current).product.id,piece.id);assert.equal(h.calls.productReads.length,1);assert.ok(h.button('End voice'));
});
test('helper download failure leaves native checked search usable while actions fail closed',async t=>{
  const h=fixture(t,{helper:false});await h.ready();const pending=h.tool('inspect_jewellery',{handle:piece.handle});await settle();h.root.querySelector('script[src$="brites-concierge-voice-actions.js"]').dispatchEvent(new h.w.Event('error'));assert.ok((await pending).error);assert.equal(h.calls.productReads.length,0);const result=await h.tool('find_jewellery',{message:'A necklace for graduation'});assert.equal(result.verified,true);assert.equal(result.products[0].id,piece.id);assert.ok(h.button('End voice'));
});
for(const currency of ['CAD','USD'])test('normal storefront inspect/review use current '+currency+' prices without reverting to catalogue USD values',async t=>{
  const h=fixture(t,{sandbox:false,storefrontCurrency:currency});await h.ready();assert.equal(h.saved().products[0].currency,currency);h.setMarket({currency,variants:piece.variants.map(v=>({id:Number(v.numericId),price:(v.numericId==='101'?79:89)*100,available:true}))});const inspected=await h.tool('inspect_jewellery',{handle:piece.handle});assert.equal(inspected.product.currency,currency);assert.equal(inspected.product.minPrice,79);assert.equal(inspected.product.variants[0].price,79);assert.equal(h.saved().products[0].minPrice,74,'read-only inspect leaves the previous card intact');
  const context=h.speak(exactReview),prepared=await h.tool('prepare_jewellery_action',{handle:piece.handle,action:'review',variantId:silver16.id},context);assert.equal(prepared.prepared,true);assert.equal(prepared.variant.price,79);assert.equal(prepared.variant.currency,currency);assert.match(h.root.querySelector('.review').textContent,new RegExp('79\\.00 '+currency));assert.equal(h.saved().products[0].currency,currency);assert.equal(h.saved().products[0].minPrice,79);assert.equal(h.calls.requests.some(r=>r.url.includes('/cart/add.js')),false);assert.equal(h.w.sessionStorage.getItem('brites-sandbox-cart'),null);assert.ok(h.button('End voice'));
});
test('unavailable market option retains current price and false availability while another exact available option can be reviewed',async t=>{
  const h=fixture(t,{sandbox:false});await h.ready();h.setMarket({variants:piece.variants.map(v=>({id:Number(v.numericId),price:(v.numericId==='102'?72:79)*100,available:v.numericId!=='102'}))});const inspected=await h.tool('inspect_jewellery',{handle:piece.handle});assert.equal(inspected.product.variants.length,3);assert.equal(inspected.product.variants[1].available,false);assert.equal(inspected.product.variants[1].price,72);assert.equal(inspected.product.variantsComplete,true);assert.equal(inspected.product.minPrice,79);const context=h.speak(exactReview);assert.equal((await h.tool('prepare_jewellery_action',{handle:piece.handle,action:'review',variantId:silver16.id},context)).prepared,true);assert.equal(h.root.querySelectorAll('select option')[1].disabled,true);
});
for(const [name,variants] of [
  ['missing option',piece.variants.filter(v=>v.numericId!=='102').map(v=>({id:Number(v.numericId),price:7900,available:true}))],
  ['extra option',[...piece.variants.map(v=>({id:Number(v.numericId),price:7900,available:true})),{id:199,price:8900,available:true}]],
  ['malformed option',piece.variants.map(v=>({id:Number(v.numericId),price:v.numericId==='102'?'7900':7900,available:true}))]
])test('market '+name+' makes the current list incomplete and blocks native review',async t=>{
  const h=fixture(t,{sandbox:false});await h.ready();h.setMarket({variants});const inspected=await h.tool('inspect_jewellery',{handle:piece.handle});assert.equal(inspected.product.variantsComplete,false);const context=h.speak(exactReview);assert.ok((await h.tool('prepare_jewellery_action',{handle:piece.handle,action:'review',variantId:silver16.id},context)).error);assert.equal(h.root.querySelector('.review'),null);assert.equal(h.calls.requests.some(r=>r.url.includes('/cart/add.js')),false);
});
test('a currently unavailable chosen option cannot be prepared despite earlier catalogue availability',async t=>{
  const h=fixture(t,{sandbox:false});await h.ready();h.setMarket({variants:piece.variants.map(v=>({id:Number(v.numericId),price:7900,available:v.numericId!=='101'}))});const context=h.speak(exactReview);assert.ok((await h.tool('prepare_jewellery_action',{handle:piece.handle,action:'review',variantId:silver16.id},context)).error);assert.equal(h.root.querySelector('.review'),null);
});
test('failed current market read never substitutes USD catalogue prices or changes the displayed CAD card',async t=>{
  const h=fixture(t,{sandbox:false});await h.ready();const card=h.root.querySelector('.card');h.setMarket({ok:false});assert.ok((await h.tool('inspect_jewellery',{handle:piece.handle})).error);const context=h.speak(exactReview);assert.ok((await h.tool('prepare_jewellery_action',{handle:piece.handle,action:'review',variantId:silver16.id},context)).error);assert.equal(h.root.querySelector('.card'),card);assert.equal(h.saved().products[0].currency,'CAD');assert.equal(h.root.querySelector('.review'),null);
});
test('new speech cancels a pending presentment-currency read and its late response cannot prepare a review',async t=>{
  let pendingMarket=null;const h=fixture(t,{sandbox:false,marketRead:()=>pendingMarket?.promise});await h.ready();pendingMarket=deferred();const context=h.speak(exactReview),prepared=h.tool('prepare_jewellery_action',{handle:piece.handle,action:'review',variantId:silver16.id},context);await settle();const request=h.calls.requests.filter(r=>r.url.endsWith('/cart.js')).at(-1);h.speak('',true);assert.equal(request.signal.aborted,true);assert.match((await prepared).error,/cancelled/);pendingMarket.resolve();await settle();assert.equal(h.root.querySelector('.review'),null);assert.equal(h.saved().products[0].currency,'CAD');
});
for(const [name,options,expected] of [
  ['explicit isolated route',{path:'/concierge-actions-qa.html',fixtureFlag:true},'fixture'],
  ['normal sandbox with fixture flag',{fixtureFlag:true},'native'],
  ['QA route without fixture flag',{path:'/concierge-actions-qa.html'},'native'],
  ['QA route without sandbox flag',{path:'/concierge-actions-qa.html',fixtureFlag:true,sandbox:false},'native']
])test('synthetic adapter gate: '+name,async t=>{
  const h=fixture(t,options);h.w.BritesConcierge.open();await h.activate();assert.equal(h.calls.fixtureFactory,expected==='fixture'?1:0);assert.equal(h.calls.nativeFactory,expected==='native'?1:0);assert.equal(h.calls.start,1);
});
