'use strict';

// Real mounted host + bridge + widget. Public catalogue and network responses
// are synthetic; avatar hardware, speech recognition and live commerce are
// deliberately outside these DOM shopping-journey assertions.
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const {JSDOM,VirtualConsole}=require('jsdom');
const project=path.resolve(__dirname,'../..'),read=name=>fs.readFileSync(path.join(project,name),'utf8'),clone=value=>JSON.parse(JSON.stringify(value));
const settle=async()=>{await new Promise(setImmediate);await new Promise(setImmediate);};
function deferred(){let resolve;const promise=new Promise(done=>{resolve=done;});return {promise,resolve};}
async function eventually(check,label){for(let n=0;n<40;n++){if(check())return;await settle();}assert.ok(check(),label);}
function product(id,title,type='Necklace',motif='fox',{silver=47.50,gold=62.25,solid=495,engraving=true}={}){
  const handle='journey43-'+id,axis=type==='Necklace'?'Necklace Length':'Hoop Size',sizes=type==='Necklace'?['16 inch','18 inch']:['8 mm','10 mm'],metals=['Sterling Silver','14/20 Gold Filled','14K Solid Gold'],personalization=engraving?['No engraving','Hand engraved']:['No engraving'];
  const variants=[];for(let m=0;m<metals.length;m++)for(let size=0;size<sizes.length;size++)for(let custom=0;custom<personalization.length;custom++){const options=[{name:'Metal',value:metals[m]},{name:axis,value:sizes[size]},...(engraving?[{name:'Personalization',value:personalization[custom]}]:[])];variants.push({id:'gid://shopify/ProductVariant/'+(id*100+m*20+size*2+custom+1),numericId:String(id*100+m*20+size*2+custom+1),title:options.map(o=>o.value).join(' / '),price:[silver,gold,solid][m]+size*4+custom*12,available:true,options});}
  return {id:'gid://shopify/Product/'+id,handle,title,type,tags:['motif:'+motif],description:'The '+motif+' charm measures 13.5 mm wide and 17 mm high. The surface is brushed. This published listing also mentions butterfly earrings as a separate cross-sell.',url:'https://britesjewelry.com/products/'+handle,currency:'USD',checkedAt:Date.now(),detailState:'checked',variantsComplete:true,image:'https://cdn.shopify.com/'+handle+'-1.jpg',images:[1,2,3].map(index=>({url:'https://cdn.shopify.com/'+handle+'-'+index+'.jpg',altText:title+' view '+index})),options:[{name:'Metal',values:metals},{name:axis,values:sizes},...(engraving?[{name:'Personalization',values:personalization}]:[])],variants};
}
function catalogue(){return [product(43401,'Fox Charm Necklace'),product(43402,'Fox Outline Necklace','Necklace','fox',{silver:42,gold:58}),product(43403,'Fox Hoop Earrings','Earrings','fox',{silver:43,gold:55,engraving:false}),product(43404,'Butterfly Necklace','Necklace','butterfly'),product(43405,'Moon Earrings','Earrings','moon',{engraving:false})];}
async function fixture(t,{rows=catalogue()}={}){
  const errors=[],vc=new VirtualConsole();vc.on('jsdomError',error=>errors.push(error));const dom=new JSDOM(read('concierge-sandbox.html'),{url:'https://preview.example/concierge-sandbox.html',runScripts:'outside-only',pretendToBeVisual:true,virtualConsole:vc}),w=dom.window,d=w.document,requests=[];let productGate=null;
  delete d.body.dataset.catalogueSeed;w.matchMedia=()=>({matches:false,addEventListener(){},removeEventListener(){}});w.HTMLElement.prototype.scrollIntoView=function(){};w.scrollTo=function(){};
  w.fetch=async(raw,init={})=>{const url=new URL(raw,w.location.href);requests.push({url,init});let body;if(url.pathname==='/api/growth/catalogue')body={live:true,checkedAt:Date.now(),products:rows,pageInfo:{hasNextPage:false,endCursor:null}};else if(url.pathname==='/api/growth/inventory')body={live:true,checkedAt:Date.now(),products:rows.map(p=>({...p,checkedAt:Date.now()})),inventory:{schema:1,total:rows.length,offset:0,limit:24,loaded:rows.length,detailsLoaded:rows.length,ready:true,partial:false,expiresAt:Date.now()+300000},pageInfo:{hasNextPage:false,nextOffset:null}};else if(url.pathname==='/api/growth/product'){if(productGate)await productGate(url,init);body={live:true,checkedAt:Date.now(),product:rows.find(p=>p.handle===url.searchParams.get('handle'))};}else if(url.pathname==='/api/growth/storefront-services')body={schema:1,guidance:{},conflicts:[],offers:{items:[]}};else if(url.pathname==='/api/growth/knowledge')body={live:true,meanings:[],checkedAt:Date.now()};else if(['/api/growth/events','/api/concierge-voice'].includes(url.pathname))body={ok:true,enabled:true,nativeAudio:true,publicDemo:true};else if(url.pathname==='/api/concierge')body={live:true,reply:'FALLBACK_SENTINEL43',products:[],meanings:[],preferences:{},preserveSelection:true};else throw Error('Unexpected synthetic journey route '+url.pathname);return {ok:true,json:async()=>clone(body)};};
  for(const name of ['brites-catalogue-intents.js','brites-concierge-shopping-guide.js','brites-storefront-bridge.js','concierge-sandbox.js'])w.eval(read(name));await settle();await w.BritesSandboxStorefront.preloadInventory();await settle();
  w.BritesConciergeAvatar={create(){return {setState(){},setEmotion(){},setVisible(){},setPaused(){},retry(){},triggerGreeting(){},clearFocus(){},focusProduct(){},setLevel(){},setSpeechSignal(){},clearProduct(){},showProduct(){},cancelPerformance(){},setFloating(){},cue(){},destroy(){}};}};
  const script=d.createElement('script');script.src='/brites-concierge.js';script.dataset.sandbox='true';Object.defineProperty(d,'currentScript',{get:()=>script});w.eval(read('brites-concierge.js'));await settle();w.BritesConcierge.open({focus:false});await settle();t.after(()=>{w.BritesConcierge.close();w.close();});
  return {w,d,rows,requests,errors,store:w.BritesSandboxStorefront,command:text=>w.BritesConcierge.sendShopperCommand(text),get root(){return d.querySelector('brites-concierge').shadowRoot;},cart:()=>JSON.parse(w.sessionStorage.getItem('brites-sandbox-cart')||'[]'),lookup:fn=>{productGate=fn;},fallbacks:()=>requests.filter(r=>r.url.pathname==='/api/concierge'&&JSON.parse(r.init.body||'{}').message)};
}
function assertOk(result,label){assert.equal(result.ok,true,label+': '+(result.reply||result.error));assert.doesNotMatch(result.reply||'',/FALLBACK_SENTINEL43/,label+' must run through the actual connected shopping system');}

test('J01: incomplete three-dropdown journey, material advice, exact subtotal, direct add, cart edit and acknowledged simulated checkout',async t=>{
  const f=await fixture(t);assertOk(await f.command('Open Fox Charm Necklace'),'open');let result=await f.command('Add this piece to my cart');assert.equal(result.ok,false);assert.equal(f.store.snapshot().productControls.openedOption,'Metal');assert.deepEqual(f.cart(),[]);
  assertOk(await f.command('Walk me through the options'),'guided options');assert.equal(f.root.querySelector('.shopping-help').hidden,false);assert.equal(f.store.snapshot().productControls.openedOption,'Metal');
  const before=clone(f.store.snapshot().productControls);result=await f.command('Which material would you recommend for everyday wear?');assertOk(result,'material advice');assert.match(result.reply,/bonded|throughout/);assert.match(result.reply,/silver tone|warm gold/);assert.deepEqual(clone(f.store.snapshot().productControls),before,'Advice must not secretly select a material');
  assertOk(await f.command('Select 14/20 Gold Filled'),'material');assert.equal(f.store.snapshot().productControls.openedOption,'Necklace Length');assertOk(await f.command('Select 18 inch'),'length');assert.equal(f.store.snapshot().productControls.openedOption,'Personalization');assertOk(await f.command('Select No engraving'),'personalization');assert.equal(f.store.snapshot().productControls.selectionStatus,'ready');assert.deepEqual(f.cart(),[]);
  assertOk(await f.command('Set quantity to 2'),'quantity');result=await f.command('What is the price of this piece?');assertOk(result,'exact price');assert.match(result.reply,/USD 66\.25/);assert.match(result.reply,/USD 132\.50/);assert.equal(result.productFacts.handle,f.rows[0].handle);
  result=await f.command('Add it to my bag');assertOk(result,'direct add');assert.equal(result.cartChanged,true);assert.equal(f.cart().length,1);assert.equal(f.cart()[0].quantity,2);assert.equal(f.cart()[0].price,66.25);assert.match(f.cart()[0].variant,/14\/20 Gold Filled \/ 18 inch \/ No engraving/);
  assertOk(await f.command('Take me to my cart'),'cart');const lineId=f.store.snapshot().bagControls.lines[0].lineId;assert.equal(f.store.snapshot().pageKind,'bag');result=await f.command('Set Fox Charm Necklace quantity to 3 in my bag');assertOk(result,'cart quantity');assert.equal(f.cart()[0].quantity,3);assert.equal(f.store.snapshot().bagControls.lines[0].lineId,lineId);assert.equal(f.store.snapshot().bagControls.total,198.75);
  assertOk(await f.command('Take me to checkout'),'checkout');assert.equal(f.store.snapshot().pageKind,'checkout');assert.equal(f.store.snapshot().currentHandle,'');assertOk(await f.command('Show the shipping step'),'shipping');assertOk(await f.command('Choose express shipping'),'demo express');assert.equal(f.store.snapshot().checkoutControls.shipping,'express');assertOk(await f.command('Complete test checkout'),'confirmation view');assert.equal(f.store.snapshot().checkoutControls.complete,false);
  const check=f.d.querySelector('#confirm-test-checkout');check.checked=true;check.dispatchEvent(new f.w.Event('change',{bubbles:true}));[...f.d.querySelectorAll('button')].find(b=>b.textContent==='Complete test checkout').click();await eventually(()=>f.store.snapshot().checkoutControls.complete,'acknowledged completion');assert.match(f.d.querySelector('.receipt').textContent,/No order was placed, no payment was taken/);assert.equal(f.cart().length,1);assert.equal(f.requests.some(r=>/cart|checkout|order|payment/.test(r.url.pathname)&&r.init.method==='POST'),false);assert.equal(f.fallbacks().length,0);assert.deepEqual(f.errors,[]);
});

test('J02: a literal answer to the visible guided material menu selects that exact option',async t=>{
  const f=await fixture(t);await f.command('Open Fox Charm Necklace');await f.command('Help me choose this piece');const count=f.requests.length,result=await f.command('Gold filled');assertOk(result,'answer to material question');assert.deepEqual(clone(f.store.snapshot().productControls.selectedOptions),[{name:'Metal',value:'14/20 Gold Filled'}]);assert.equal(f.store.snapshot().productControls.openedOption,'Necklace Length');assert.deepEqual(f.cart(),[]);assert.equal(f.requests.length,count);
});

for(const phrase of ['Use gold filled instead','Make this gold filled','I would like the gold filled option'])test('J03: ordinary current-piece material correction — '+phrase,async t=>{
  const f=await fixture(t);await f.command('Open Fox Charm Necklace');await f.command('Select Sterling Silver');await f.command('Select 18 inch');await f.command('Select No engraving');const count=f.requests.length,result=await f.command(phrase);assertOk(result,'material correction');const pc=f.store.snapshot().productControls;assert.equal(f.store.snapshot().currentHandle,f.rows[0].handle);assert.deepEqual(clone(pc.selectedOptions),[{name:'Metal',value:'14/20 Gold Filled'},{name:'Necklace Length',value:'18 inch'},{name:'Personalization',value:'No engraving'}]);assert.equal(pc.selectionStatus,'ready');assert.deepEqual(f.cart(),[]);assert.equal(f.requests.length,count);
});

test('J04: real suggested-option button executes its literal bound choice and updates the next guided menu',async t=>{
  const f=await fixture(t);await f.command('Open Fox Charm Necklace');assertOk(await f.command('Help me choose this piece'),'show suggestions');const choices=[...f.root.querySelectorAll('.shopping-help-choices button')],choice=choices.find(b=>b.textContent==='Sterling Silver'||b.textContent==='14/20 Gold Filled');assert.ok(choice,'Guided material help must offer at least one real material choice');const expected=choice.textContent,count=f.requests.length;choice.click();await eventually(()=>f.store.snapshot().productControls.selectedOptions.some(o=>o.name==='Metal'&&o.value===expected),'suggested material is selected');assert.equal(f.store.snapshot().productControls.openedOption,'Necklace Length');assert.deepEqual(f.cart(),[]);assert.equal(f.requests.length,count);assert.deepEqual(f.errors,[]);
});

test('J05: manual native selection and quantity changes own the next exact factual answer and add',async t=>{
  const f=await fixture(t);await f.command('Open Fox Charm Necklace');const exact=f.rows[0].variants.find(v=>v.title==='Sterling Silver / 18 inch / No engraving'),select=f.d.querySelector('#piece-variant');select.value=exact.id;select.dispatchEvent(new f.w.Event('change',{bubbles:true}));const quantity=f.d.querySelector('.product-quantity input');quantity.value='3';quantity.dispatchEvent(new f.w.Event('change',{bubbles:true}));await settle();const revision=f.store.snapshot().contextRevision,count=f.requests.length,result=await f.command('What is the price of the one I selected?');assertOk(result,'manual selection price');assert.equal(result.productFacts.selectedVariant.id,exact.id);assert.equal(result.productFacts.quantity,3);assert.equal(result.productFacts.itemTotalPrice,154.50);assert.match(result.reply,/USD 51\.50/);assert.match(result.reply,/USD 154\.50/);assert.equal(f.store.snapshot().contextRevision,revision);assert.equal(f.requests.length,count);assertOk(await f.command('Add this to my cart'),'manual choice add');assert.equal(f.cart()[0].variantId,exact.numericId);assert.equal(f.cart()[0].quantity,3);assert.deepEqual(f.errors,[]);
});

test('J06: matching and similar pieces are prepared for the actual listing without changing it or its choices',async t=>{
  const f=await fixture(t);await f.command('Open Fox Charm Necklace');await f.command('Select 14/20 Gold Filled');await f.command('Select 18 inch');await f.command('Select No engraving');const before=clone(f.store.snapshot()),count=f.requests.length;let result=await f.command('What earrings would go with this?');assertOk(result,'matching earrings');assert.deepEqual(clone(result.recommendations).map(row=>row.handle),[f.rows[2].handle]);assert.match(result.reply,/14\/20 Gold Filled/);assert.match(result.reply,/(?:USD 55\.00|\$55\.00 USD)/);assert.deepEqual(clone(f.store.snapshot()),before);assert.match(f.root.querySelector('.shopping-help').textContent,/Fox Hoop Earrings/);
  result=await f.command("I'm not sure about this one");assertOk(result,'uncertainty');assert.equal(result.recommendations[0].handle,f.rows[1].handle,'The closest shared motif ranks first');for(const row of result.recommendations){const p=f.rows.find(p=>p.handle===row.handle),v=p?.variants.find(v=>v.id===row.variantId);assert.equal(p?.type,'Necklace');assert.equal(v?.available,true);assert.equal(v?.options.find(o=>o.name==='Metal').value,'14/20 Gold Filled');assert.equal(row.price,v.price);assert.equal(row.currency,'USD');assert.equal(row.variantTitle,v.title);assert.equal(row.requiresFreshCheck,true);assert.ok(Number.isFinite(row.checkedAt));assert.match(row.why,/motif|design/);}assert.deepEqual(clone(f.store.snapshot()),before);assert.deepEqual(f.cart(),[]);assert.equal(f.requests.length,count);
  await f.store.execute({type:'open',handle:f.rows[3].handle});result=await f.command('What size is the charm?');assertOk(result,'actual new listing size');assert.equal(result.productFacts.handle,f.rows[3].handle);assert.match(result.reply,/13\.5 mm.*17 mm/);assert.equal(f.store.snapshot().currentHandle,f.rows[3].handle);assert.doesNotMatch(f.root.querySelector('.shopping-help').textContent,/Fox Hoop Earrings/);
});

test('J07: No thanks stays dismissed across pointer, option, gallery and page changes without auto narration or restart',async t=>{
  const f=await fixture(t);await f.command('Open Fox Charm Necklace');await f.command('Help me choose this piece');const help=f.root.querySelector('.shopping-help'),dismiss=[...help.querySelectorAll('button')].find(b=>b.textContent==='No thanks');assert.ok(dismiss);dismiss.click();assert.equal(help.hidden,true);const count=f.requests.length;f.d.querySelector('.option-menu-trigger').click();f.d.querySelector('[data-option-name="Metal"] [data-option-value="Sterling Silver"]').click();f.d.querySelector('.image-thumbs button:nth-child(2)').click();f.d.querySelector('.product-copy').dispatchEvent(new f.w.Event('pointerover',{bubbles:true}));await settle();assert.equal(help.hidden,true);assert.equal(f.requests.length,count);await f.store.execute({type:'open',handle:f.rows[3].handle});assert.equal(help.hidden,true);assert.equal(f.root.host.dataset.open,'true');f.w.BritesConcierge.close();assert.equal(f.root.host.dataset.open,'false');await f.store.execute({type:'open',handle:f.rows[0].handle});f.d.dispatchEvent(new f.w.CustomEvent('brites-storefront:context'));await settle();assert.equal(f.root.host.dataset.open,'false');assert.equal(help.hidden,true);assert.equal(f.requests.some(r=>r.url.pathname==='/api/concierge-voice'&&r.init.method==='POST'),false);assert.deepEqual(f.errors,[]);
});

test('J08: closing the widget cancels a pending exact add; stale responses never add or reopen the guide',async t=>{
  const f=await fixture(t);await f.command('Open Fox Charm Necklace');await f.command('Select Sterling Silver');await f.command('Select 18 inch');await f.command('Select No engraving');const gate=deferred(),entered=deferred();f.lookup(async()=>{entered.resolve();await gate.promise;});const pending=f.command('Add it to my cart');await entered.promise;f.w.BritesConcierge.close();const result=await pending;assert.equal(result.ok,false);assert.deepEqual(f.cart(),[]);assert.equal(f.root.host.dataset.open,'false');gate.resolve();await settle();assert.deepEqual(f.cart(),[]);assert.equal(f.root.host.dataset.open,'false');assert.deepEqual(f.errors,[]);
});

test('J09: gold-filled construction question receives a useful distinction from solid gold without changing selections',async t=>{
  const f=await fixture(t);await f.command('Open Fox Charm Necklace');const before=clone(f.store.snapshot()),count=f.requests.length,result=await f.command('Is gold filled solid gold?');assertOk(result,'gold construction');assert.match(result.reply,/bonded|surface.*base metal/);assert.match(result.reply,/throughout|gold alloy throughout/);assert.equal(result.materialGuidance?.productSpecificGuarantees,false);assert.deepEqual(clone(f.store.snapshot()),before);assert.equal(f.requests.length,count);
});

test('J10: this exact choice refers to the configured current listing and quantity',async t=>{
  const f=await fixture(t);await f.command('Open Fox Charm Necklace');await f.command('Select 14/20 Gold Filled');await f.command('Select 18 inch');await f.command('Select No engraving');await f.command('Set quantity to 2');const before=clone(f.store.snapshot()),count=f.requests.length,result=await f.command('How much is this exact choice?');assertOk(result,'this exact choice');assert.equal(result.productFacts.handle,f.rows[0].handle);assert.match(result.reply,/USD 66\.25/);assert.match(result.reply,/USD 132\.50/);assert.deepEqual(clone(f.store.snapshot()),before);assert.equal(f.requests.length,count);
});

test('J11: polite short answers complete the visible walkthrough without repeating selection verbs',async t=>{
  const f=await fixture(t);
  assertOk(await f.command('Open Fox Charm Necklace'),'open');
  assertOk(await f.command('Help me choose this piece'),'walkthrough');
  const before=f.requests.length;
  assertOk(await f.command('Gold filled please'),'material answer');
  assert.equal(f.store.snapshot().productControls.openedOption,'Necklace Length');
  assertOk(await f.command('18 inches please'),'published inch answer');
  assert.equal(f.store.snapshot().productControls.openedOption,'Personalization');
  assertOk(await f.command('No engraving please'),'personalization answer');
  assertOk(await f.command('Set the quantity to three'),'spoken whole quantity');
  assert.deepEqual(clone(f.store.snapshot().productControls.selectedOptions),[
    {name:'Metal',value:'14/20 Gold Filled'},
    {name:'Necklace Length',value:'18 inch'},
    {name:'Personalization',value:'No engraving'}
  ]);
  assert.equal(f.store.snapshot().productControls.quantity,3);
  assert.equal(f.store.snapshot().productControls.itemTotalPrice,198.75);
  assert.equal(f.store.snapshot().productControls.selectionStatus,'ready');
  assert.deepEqual(f.cart(),[],'Choosing complete options never implies an add request');
  assert.equal(f.requests.length,before,'All visible literal choices are local');
  const added=await f.command('Add this to my cart');
  assertOk(added,'add after short answers');
  assert.equal(added.cartChanged,true);
  assert.equal(f.cart()[0].quantity,3);
  assert.equal(f.cart()[0].variant,'14/20 Gold Filled / 18 inch / No engraving');
  assert.equal(f.fallbacks().length,0);
  assert.deepEqual(f.errors,[]);
});

test('J12: manual cart quantity changes own the next checkout total and recommendations remain tied to the bag page',async t=>{
  const f=await fixture(t);
  for(const text of ['Open Fox Charm Necklace','Select Sterling Silver','Select 18 inch','Select No engraving','Add this to my cart','Take me to my cart'])assertOk(await f.command(text),text);
  const before=f.store.snapshot().bagControls.lines[0].lineId;
  const quantity=f.d.querySelector('.bag-quantity input');
  quantity.value='4';quantity.dispatchEvent(new f.w.Event('change',{bubbles:true}));
  await eventually(()=>f.store.snapshot().bagControls.lines[0].quantity===4,'manual cart change is verified');
  assert.equal(f.cart()[0].quantity,4);
  assert.equal(f.store.snapshot().bagControls.lines[0].lineId,before);
  assert.equal(f.store.snapshot().bagControls.total,206);
  assert.equal(f.store.snapshot().currentHandle,'');
  assert.equal(f.store.snapshot().productControls,null);
  assert.doesNotMatch(f.root.querySelector('.shopping-help').textContent,/Fox Hoop Earrings|Choose Metal/);
  assertOk(await f.command('Take me to checkout'),'checkout after manual bag edit');
  assert.match(f.d.querySelector('.checkout-line').textContent,/quantity 4/);
  assert.match(f.d.querySelector('.checkout-line').textContent,/206\.00/);
  assert.equal(f.store.snapshot().checkoutControls.complete,false);
  assert.equal(f.store.snapshot().checkoutControls.acknowledged,false);
  assert.equal(f.store.snapshot().currentHandle,'');
  assert.equal(f.store.snapshot().productControls,null);
  assert.equal(f.fallbacks().length,0);
  assert.deepEqual(f.errors,[]);
});

test('J13: published-choice console offers available literal values and adds only a ready exact selection',async t=>{
  const rows=catalogue();rows[0].variants.forEach(v=>{if(v.options.some(o=>o.name==='Metal'&&o.value==='14/20 Gold Filled'))v.available=false;});
  const f=await fixture(t,{rows}),labels=()=>[...f.d.querySelectorAll('#guide-commands button')].map(b=>b.textContent);
  assertOk(await f.command('Open Fox Charm Necklace'),'open with unavailable material');
  assert.equal(labels().includes('Choose 14/20 Gold Filled'),false,'A published unavailable value is not offered as a working console choice');
  assert.equal(labels().includes('Choose Sterling Silver'),true,'Available incomplete choices remain usable');
  assert.equal(labels().includes('Add to my bag'),false,'An incomplete selection cannot offer Add');
  assertOk(await f.command('Open the metal menu'),'inspect actual native menu');
  assert.equal(f.d.querySelector('[data-option-name="Metal"] [data-option-value="14/20 Gold Filled"]').disabled,true,'The literal sold-out option remains visible and clearly disabled in the actual menu');
  for(const text of ['Select Sterling Silver','Select 18 inch','Select No engraving'])assertOk(await f.command(text),text);
  assert.equal(f.store.snapshot().productControls.selectionStatus,'ready');
  assert.equal(labels().includes('Add to my bag'),true);
  assert.deepEqual(f.cart(),[]);
  const heldRows=catalogue();heldRows[0].cartHold=true;const held=await fixture(t,{rows:heldRows});
  for(const text of ['Open Fox Charm Necklace','Select Sterling Silver','Select 18 inch','Select No engraving'])assertOk(await held.command(text),text);
  assert.ok(held.store.snapshot().productControls.selectedVariant);
  assert.equal(held.store.snapshot().productControls.selectionStatus,'held');
  assert.equal([...held.d.querySelectorAll('#guide-commands button')].some(b=>b.textContent==='Add to my bag'),false,'A held exact selection cannot offer Add');
  assert.deepEqual(held.cart(),[]);
  assert.deepEqual(f.errors,[]);assert.deepEqual(held.errors,[]);
});
