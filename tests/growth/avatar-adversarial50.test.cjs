'use strict';

// Independent adversarial checks of production widget, host, resolver, native
// client and (where requested) avatar fallback controller. Network, media,
// recognition and scrolling are synthetic; no physical speech/commerce claim.
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs');
const Avatar=require('../../brites-concierge-avatar.js');
const Shopify=require('../../brites-shopify-storefront-adapter.js');
const {publishedProduct,settle}=require('./native-continuity48-fixture.cjs');
const copy=value=>JSON.parse(JSON.stringify(value));
let nativeFixture;
function fixtureEntry(){
 if(nativeFixture)return nativeFixture;
 const Module=require('node:module'),path=require('node:path'),name=require.resolve('./native-continuity48-fixture.cjs'),source=fs.readFileSync(name,'utf8');
 const signature="permissionState='prompt'}={})",mount="w.eval(source['brites-concierge.js']);const root",origin="url:'https://preview.example/concierge-sandbox.html'+query",delivery='get client(){return client;}',finalizer='const result=await options.onFinalizedTurn(input);finals.push({input,result});return result;',typedGuard="w.BritesConcierge.sendShopperCommand=()=>{typedAttempts++;throw Error('Typed shopper handler is forbidden in native acceptance47');};root.querySelector('.composer form').onsubmit=()=>{typedAttempts++;throw Error('Typed composer is forbidden in native acceptance47');};";
 for(const anchor of [signature,mount,origin,delivery,finalizer,typedGuard])assert.equal(source.split(anchor).length,2,'Only explicit synthetic fixture hooks and observed callback errors are added');
 const helper=new Module(name,module);helper.filename=name;helper.paths=Module._nodeModulePaths(path.dirname(name));
 helper._compile(source.replace(signature,"permissionState='prompt',beforeWidget=null,fixtureOrigin='https://preview.example',allowTyped=false}={})").replace(origin,"url:fixtureOrigin+'/concierge-sandbox.html'+query").replace(mount,"await beforeWidget?.({w,d});w.eval(source['brites-concierge.js']);const root").replace(typedGuard,'if(!allowTyped){'+typedGuard+'}').replace(delivery,'get providerMessageHandler(){return channel.onmessage;},'+delivery).replace(finalizer,'let result;try{result=await options.onFinalizedTurn(input);}catch(error){finals.push({input,result:{fixtureObservedError:String(error?.stack||error)}});throw error;}finals.push({input,result});return result;'),name);
 return nativeFixture=helper.exports.fixture;
}
function catalogue({engraving=false}={}){
 const rows=Array.from({length:120},(_,i)=>publishedProduct(i)),p=rows[10];
 p.options=[{name:'Metal Choice',values:['Sterling Silver','14k Gold Filled','14k Solid Gold']},{name:'Length',values:['16 inches','18 inches']}];
 if(engraving)p.options.push({name:'Engraving',values:['No','Yes']});
 let serial=0;p.variants=p.options[0].values.flatMap((metal,m)=>p.options[1].values.flatMap((length,n)=>(engraving?['No','Yes']:[null]).map(wording=>{
  const numericId=String(500000+(serial++));return {id:'gid://shopify/ProductVariant/'+numericId,numericId,title:[metal,length,wording].filter(Boolean).join(' / '),price:51+m*101+n*7+(wording==='Yes'?12:0),available:true,options:[{name:'Metal Choice',value:metal},{name:'Length',value:length}].concat(wording?[{name:'Engraving',value:wording}]:[])};
 })));
 p.images=[1,2,3].map(n=>({url:'https://cdn.shopify.com/elephant-50-'+n+'.jpg',alt:'Synthetic Elephant image '+n}));
 p.image=p.images[0].url;
 return rows;
}
const cartItem=(p,v,quantity=1)=>({productId:p.id,title:p.title,variantId:v.numericId,variant:v.title,variantOptions:copy(v.options),price:v.price,currency:p.currency,...(quantity!==1?{quantity}:{})});
const savedCart=rows=>[cartItem(rows[10],rows[10].variants[0],2),cartItem(rows[1],rows[1].variants[0],3)];
async function fixture(t,{realAvatar=false,beforeWidget:customBeforeWidget=null,...options}={}){
 const calls={scroll:[],reveal:[],avatar:[]};let avatar;
 const f=await fixtureEntry()(t,{customProducts:catalogue(),...options,async beforeWidget({w,d}){
  let top=0;Object.defineProperty(w,'scrollY',{get:()=>top,configurable:true});
  Object.defineProperty(d.documentElement,'scrollHeight',{get:()=>4200,configurable:true});
  w.scrollTo=value=>{top=typeof value==='number'?value:Number(value?.top)||0;calls.scroll.push({top,options:copy(value)});};
  w.HTMLElement.prototype.scrollIntoView=function(options){calls.reveal.push({node:this,section:this.dataset.storeSection||null,options:copy(options||{})});};
  w.eval(fs.readFileSync(require.resolve('../../brites-concierge-expression.js'),'utf8'));
  if(realAvatar)w.BritesConciergeAvatar={create(options){
   avatar=Avatar.create({...options,loadScene:async()=>{throw Error('Synthetic unavailable GPU in adversarial50');}});
   for(const method of ['setState','setEmotion','setExpression','perform','cue','cancelPerformance']){const original=avatar[method];avatar[method]=(...args)=>{calls.avatar.push({method,args:copy(args)});return original.apply(avatar,args);};}
   return avatar;
  }};
  await customBeforeWidget?.({w,d});
 }});
 if(avatar)await avatar.ready;t.after(()=>avatar?.destroy());return {...f,calls,get avatar(){return avatar;}};
}
async function until(predicate,reason){const deadline=Date.now()+3000;while(Date.now()<deadline&&!predicate()){await settle();await new Promise(resolve=>setTimeout(resolve,2));}assert.ok(predicate(),reason);}
async function say(f,text){const out=await f.say(text);await until(()=>f.finals.some(row=>row.input.inputItemId===out.input.itemId),'The actual finalized native request completes: '+text);return f.finals.find(row=>row.input.inputItemId===out.input.itemId).result;}
const currentBag=f=>copy(f.store.snapshot().bagControls.lines);
const button=(root,text)=>{const b=[...root.querySelectorAll('button')].find(b=>b.textContent===text);assert.ok(b,'Actual visible button: '+text);return b;};
const publicPackets=f=>f.packets.filter(p=>p.item?.content?.[0]?.text?.startsWith('Public website UI')).map(p=>p.item.content[0].text);
const point=(f,node)=>{assert.ok(node);node.dispatchEvent(new f.w.Event('pointerdown',{bubbles:true,composed:true}));};
async function chooseAndAdd(f){for(const text of ['Select Sterling Silver','Select 16 inches','Set quantity to 2','Add this piece to my bag'])assert.equal((await say(f,text)).ok,true,text);}
async function nativeCartFixture(t,options={}){
 let adapter;const native={state:{currency:'USD',item_count:2,note:'PRIVATE_NATIVE50',attributes:{wrapping:'yes'},items:[{key:'5001:actual50',product_id:500,variant_id:5001,product_title:'Fern Necklace',variant_title:'Silver / 16 inches',quantity:2,final_price:4000,properties:{}}]},calls:[],discountClicks:0,checkoutClicks:0,termsChanges:0,navigation:[]};
 const f=await fixture(t,{...options,fixtureOrigin:'https://britesjewelry.com',async beforeWidget({w,d}){
  w.history.replaceState({},'','/cart');d.querySelector('main').outerHTML='<main id="MainContent"><form id="bjCartForm" action="/cart" method="post"><div id="bjCartItems"><article class="bj-cp__row" data-key="5001:actual50" data-title="Fern Necklace"><a class="cart__product-name">Fern Necklace</a><div data-bjcp-qtywrap><input class="cart__product-qty" name="updates[]" data-id="5001:actual50" value="2"></div><span data-bjcp-linetotal>80.00</span><a data-bjcp-remove href="/cart/change?id=5001:actual50&quantity=0">Remove</a></article></div><input id="bjcpDiscIn" type="text"><button id="bjcpDiscApply" type="button">Apply discount</button><span id="bjcpDiscMsg"></span><input id="CartPageAgree" type="checkbox"><label for="CartPageAgree">I agree to the cart terms and conditions</label><button type="submit" name="checkout">Checkout</button></form></main>';
  d.querySelector('#bjcpDiscApply').addEventListener('click',()=>{native.discountClicks++;const code=d.querySelector('#bjcpDiscIn').value.toUpperCase();w.sessionStorage.setItem('bjcpDisc',code);const message=d.querySelector('#bjcpDiscMsg');message.classList.add('ok');message.textContent='Code '+code+' will be applied at checkout';});d.querySelector('[name="checkout"]').addEventListener('click',event=>{native.checkoutClicks++;event.preventDefault();});d.querySelector('#CartPageAgree').addEventListener('change',()=>native.termsChanges++);
  adapter=Shopify.create({window:w,root:'/',theme:'brites-v1',async fetch(url,init={}){native.calls.push({url,init});assert.notEqual(init.method,'POST','Consent/discount staging may not write cart lines or submit checkout');return {ok:true,json:async()=>copy(native.state)};},navigate:url=>native.navigation.push(url)});assert.ok(adapter);w.BritesStorefrontAdapter=adapter;await adapter.refresh();
 }});t.after(()=>adapter.destroy());return {...f,native,adapter};
}

test('native cart line quantity, literal option changes, exact removal and undo preserve unrelated rows and subtotals',async t=>{
 const f=await fixture(t,{query:'?cart=1',savedCart}),ids=currentBag(f).map(p=>p.lineId),other=copy(f.cart()[1]);
 assert.equal((await say(f,'Set quantity of the first item in my cart to 4')).ok,true);assert.equal(f.cart()[0].quantity,4);assert.deepEqual(f.cart()[1],other);
 assert.equal((await say(f,'Change the first item in my cart to 18 inches')).ok,true);assert.match(f.cart()[0].variant,/18 inches/);assert.equal(f.cart()[0].quantity,4);assert.equal(f.d.querySelector('[data-bag-option="Length"]').value,'18 inches');assert.deepEqual(f.cart()[1],other);
 assert.equal((await say(f,'Change the first item in my cart to 14k Solid Gold')).ok,true);assert.match(f.cart()[0].variant,/14k Solid Gold.*18 inches/);assert.deepEqual(f.cart()[1],other);
 const totals=f.store.snapshot().bagControls;assert.equal(totals.total,f.cart().reduce((sum,p)=>sum+p.price*(p.quantity||1),0));assert.deepEqual(currentBag(f).map(p=>p.lineId),ids);
 assert.equal((await say(f,'Remove the first item from my cart')).ok,true);assert.deepEqual(f.cart(),[other]);
 assert.equal((await say(f,'Undo that')).ok,true);assert.equal(f.cart().length,2);assert.equal(f.cart()[0].quantity,4);assert.match(f.cart()[0].variant,/14k Solid Gold.*18 inches/);assert.deepEqual(f.cart()[1],other);f.assertNativeOnly();
});

test('opening a real cart Length dropdown and spoken ordinal selects only that exact current menu',async t=>{
 const f=await fixture(t,{query:'?cart=1',savedCart}),other=copy(f.cart()[1]),before=copy(f.cart());
 const opened=await say(f,'Open the Length dropdown for the first item in my cart');assert.equal(opened.ok,true,JSON.stringify(opened));assert.deepEqual(f.cart(),before);
 const menu=f.d.querySelector('[data-bag-line] [data-bag-option="Length"]');assert.ok(menu);const list=f.d.querySelector('[data-bag-line] [data-bag-menu="Length"]');assert.equal(list.hidden,false);assert.equal(list.previousElementSibling.getAttribute('aria-expanded'),'true');assert.ok(f.calls.reveal.some(row=>row.node===menu||row.node.contains(menu)),'The actual cart menu is brought into view');
 f.d.querySelector('[data-bag-line] .bag-quantity input').focus();assert.equal(f.store.snapshot().bagControls.quantityFocused,true);await say(f,'Second');assert.deepEqual(f.cart(),before,'Bare ordinal cannot borrow a dropdown after attention moved to quantity');
 const choice=await say(f,'Select the second option');assert.equal(choice.ok,true,JSON.stringify(choice));assert.match(f.cart()[0].variant,/18 inches/);assert.equal(f.d.querySelector('[data-bag-option="Length"]').value,'18 inches');assert.deepEqual(f.cart()[1],other);
 assert.equal((await say(f,'Close the dropdown')).ok,true);const changed=copy(f.cart());await say(f,'Select the first option');assert.deepEqual(f.cart(),changed,'A closed menu cannot silently lend ordinal authority');f.assertNativeOnly();
});

test('cart menu selected values remain exact after product navigation and fresh same-tab reload',async t=>{
 const a=await fixture(t,{query:'?cart=1',savedCart});assert.equal((await say(a,'Change the first item in my cart to 18 inches')).ok,true);const selected=copy(a.cart());
 assert.equal((await say(a,'Open Cat Stud Earrings')).ok,true);assert.equal((await say(a,'Open my cart')).ok,true);assert.equal(a.d.querySelector('[data-bag-option="Length"]').value,'18 inches');assert.deepEqual(a.cart(),selected);
 const savedSession=Object.fromEntries(Array.from({length:a.w.sessionStorage.length},(_,i)=>{const key=a.w.sessionStorage.key(i);return [key,a.w.sessionStorage.getItem(key)];}));
 const b=await fixture(t,{query:'?cart=1',savedSession});assert.deepEqual(b.cart(),selected);assert.equal(b.d.querySelector('[data-bag-option="Length"]').value,'18 inches');assert.equal(b.d.querySelector('[data-bag-option="Metal Choice"]').value,'Sterling Silver');b.assertNativeOnly();
});

test('voice highlights the actual cart quantity and price targets without editing or borrowing another line',async t=>{
 const f=await fixture(t,{query:'?cart=1',savedCart}),before=copy(f.cart()),lineId=currentBag(f)[0].lineId;
 for(const section of ['quantity','price']){const from=f.calls.reveal.length;const out=await say(f,'Highlight the '+section+' of the first item in my cart');assert.equal(out.ok,true,JSON.stringify(out));assert.ok(f.calls.reveal.slice(from).some(row=>row.node.dataset.bagLine===lineId||row.node.closest('[data-bag-line]')?.dataset.bagLine===lineId),'The actual exact cart row is revealed');assert.deepEqual(f.cart(),before);}
 f.assertNativeOnly();
});

for(const phrase of ['Do not remove the first item from my cart.','The note says "remove the first item from my cart".','If I remove the first item from my cart, what happens?','Can you explain how to remove the first item from my cart?','Remove it','I said "empty my cart", but do not do it.','Empty my bag? No, keep it.','Would removing all items from the bag lose my choices?'])test('ambiguous, negated, quoted or hypothetical native cart words cannot modify rows: '+phrase,async t=>{
 const f=await fixture(t,{query:'?cart=1',savedCart}),before=copy(f.cart()),controls=f.controls.length;await say(f,phrase);assert.deepEqual(f.cart(),before);assert.equal(f.controls.length,controls);f.assertNativeOnly();
});

for(const phrase of ['Empty my bag','Remove all items from my cart'])test('an explicit native bulk clear removes exactly the current lines once and preserves gift preferences: '+phrase,async t=>{
 const f=await fixture(t,{query:'?cart=1',savedCart}),before=copy(f.cart());f.w.sessionStorage.setItem('brites-sandbox-gift-preferences',JSON.stringify({wrapping:true,giftPackage:true,giftNote:'PRIVATE_GIFT50',customIdea:'PRIVATE_DESIGN50'}));
 const preferences=f.w.sessionStorage.getItem('brites-sandbox-gift-preferences'),out=await say(f,phrase);assert.equal(out.ok,true,JSON.stringify(out));assert.deepEqual(f.cart(),[]);assert.equal(f.w.sessionStorage.getItem('brites-sandbox-gift-preferences'),preferences);assert.equal(f.store.snapshot().bagControls.itemCount,0);assert.match(f.d.querySelector('main').textContent,/bag is empty/i);
 const final=f.finals.at(-1).input;f.emit({type:'conversation.item.input_audio_transcription.completed',item_id:final.inputItemId,transcript:phrase});await settle();assert.deepEqual(f.cart(),[]);
 assert.equal((await say(f,'Undo that')).ok,true);assert.deepEqual(f.cart(),before);assert.equal(f.w.sessionStorage.getItem('brites-sandbox-gift-preferences'),preferences);f.assertNativeOnly();
});

test('native bulk clear cannot cross a delayed host delivery after newer page-aware speech',async t=>{
 const f=await fixture(t,{query:'?cart=1',savedCart}),before=copy(f.cart()),host=f.w.BritesSandboxStorefront,execute=host.execute;let release,waiting=0;const gate=new Promise(resolve=>release=resolve);
 // Local removal needs no inventory lookup. Pause delivery to the actual
 // host to exercise the real widget's cancellation signal before mutation.
 host.execute=async(action,options)=>{if(action.type==='bag-clear'){waiting++;await gate;}return execute(action,options);};
 const old=await f.say('Empty my bag');await until(()=>waiting>0,'The actual native request reaches the host-delivery boundary');await say(f,'What is in my cart?');release();await until(()=>f.finals.some(row=>row.input.inputItemId===old.input.itemId),'Interrupted clear retires');assert.deepEqual(f.cart(),before);assert.equal(f.finals.find(row=>row.input.inputItemId===old.input.itemId).result.ok,false);f.assertNativeOnly();
});

test('a directly aborted cart clear and an older complete-line snapshot cannot erase newer exact rows',async t=>{
 const f=await fixture(t,{query:'?cart=1',savedCart}),before=copy(f.cart()),ids=currentBag(f).map(line=>line.lineId),controller=new f.w.AbortController();controller.abort();assert.equal((await f.store.execute({type:'bag-clear',lineIds:ids},{signal:controller.signal})).ok,false);assert.deepEqual(f.cart(),before);
 assert.equal((await say(f,'Remove the first item from my cart')).ok,true);const newer=copy(f.cart());assert.equal((await f.store.execute({type:'bag-clear',lineIds:ids})).ok,false);assert.deepEqual(f.cart(),newer);f.assertNativeOnly();
});

test('bulk clearing refuses a silently detached cart row instead of guessing the unseen line',async t=>{
 const f=await fixture(t,{query:'?cart=1',savedCart}),before=copy(f.cart()),ids=currentBag(f).map(line=>line.lineId);f.d.querySelector('[data-bag-line]').remove();assert.equal((await f.store.execute({type:'bag-clear',lineIds:ids})).ok,false);assert.deepEqual(f.cart(),before);f.assertNativeOnly();
});

test('all visible checkout controls remain a simulation and spoken completion never clicks the acknowledgement',async t=>{
 const f=await fixture(t,{query:'?cart=1',savedCart}),before=copy(f.cart());assert.equal((await say(f,'Go to checkout')).ok,true);
 assert.equal((await say(f,'Show the shipping step')).ok,true);assert.equal(f.store.snapshot().checkoutControls.step,'shipping');assert.equal(f.d.querySelector('[data-checkout-step="shipping"]').hidden,false);
 assert.equal((await say(f,'Select demo express shipping')).ok,true);assert.equal(f.store.snapshot().checkoutControls.shipping,'express');assert.equal(f.d.querySelector('[data-demo-shipping="express"]').getAttribute('aria-pressed'),'true');
 const completion=await say(f,'Complete test checkout');assert.equal(completion.ok,true);assert.equal(f.store.snapshot().checkoutControls.acknowledged,false);assert.equal(f.store.snapshot().checkoutControls.complete,false);assert.equal(f.d.querySelector('#confirm-test-checkout').checked,false);assert.equal(button(f.d,'Complete test checkout').disabled,true);assert.deepEqual(f.cart(),before);f.assertNativeOnly();
});

for(const direction of ['down','up','top','bottom'])test('native scroll '+direction+' changes actual window target and never product choices',async t=>{
 const f=await fixture(t,{query:'?product=elephant-necklace'}),before=copy(f.store.snapshot().productControls);f.w.scrollTo({top:1200});const start=f.calls.scroll.length;
 const out=await say(f,'Scroll '+direction);assert.equal(out.ok,true,JSON.stringify(out));assert.equal(f.calls.scroll.length,start+1);const expected=direction==='top'?0:direction==='bottom'?4200-f.w.innerHeight:direction==='up'?1200-Math.max(240,f.w.innerHeight*.75):1200+Math.max(240,f.w.innerHeight*.75);assert.equal(f.calls.scroll.at(-1).top,expected);assert.deepEqual(copy(f.store.snapshot().productControls),before);assert.deepEqual(f.cart(),[]);f.assertNativeOnly();
});

test('native section highlight and gallery controls act on actual current DOM and retire on repeated navigation',async t=>{
 const f=await fixture(t,{query:'?product=elephant-necklace'});assert.equal((await say(f,'Highlight the description')).ok,true);assert.equal(f.store.snapshot().activeSection,'description');assert.ok(f.calls.reveal.some(row=>row.section==='description'));
 assert.equal((await say(f,'Show image 2')).ok,true);assert.equal(f.store.snapshot().galleryControls.selectedIndex,2);assert.equal(f.d.querySelector('[data-store-section="image"] img').getAttribute('src'),f.products[10].images[1].url);assert.equal((await say(f,'Zoom in on this image')).ok,true);assert.ok(f.d.querySelector('#storefront-image-dialog[open]'));assert.equal((await say(f,'Close the image')).ok,true);assert.equal(f.d.querySelector('#storefront-image-dialog'),null);
 for(let n=0;n<5;n++){assert.equal((await say(f,'Open Cat Stud Earrings')).ok,true);assert.equal(f.store.snapshot().currentHandle,'cat-stud-earrings');assert.equal((await say(f,'Go back')).ok,true);assert.equal(f.store.snapshot().currentHandle,'elephant-necklace');assert.equal((await say(f,'Go forward')).ok,true);assert.equal(f.store.snapshot().currentHandle,'cat-stud-earrings');assert.equal((await say(f,'Go back')).ok,true);}
 assert.equal(f.requests.filter(r=>r.body?.action==='start').length,1);assert.equal(f.requests.filter(r=>r.body?.action==='stop').length,0);assert.equal(f.microphoneCalls,1);await say(f,'What am I looking at?');assert.match(f.lastSpoken(),/Elephant Necklace/);f.assertNativeOnly();
});

test('a manually changed native cart dropdown invalidates pending provider authority to the earlier chosen variant',async t=>{
 const f=await fixture(t,{query:'?cart=1',savedCart,providerFallback:true});await f.say('Change the first item in my cart to 18 inches');const request=f.responses().at(-1),responseId='adversarial50-old-cart-reply';f.emit({type:'response.created',response:{id:responseId,metadata:request.response.metadata}});
 const line=currentBag(f)[0],menu=f.d.querySelector('[data-bag-option="Metal Choice"]');menu.value='14k Solid Gold';menu.dispatchEvent(new f.w.Event('change',{bubbles:true}));await until(()=>/14k Solid Gold/.test(f.cart()[0].variant),'Actual manual dropdown edit finishes');const changed=copy(f.cart()),controls=f.controls.length;
 const item={id:'adversarial50-old-item',type:'function_call',status:'completed',name:'control_storefront',call_id:'adversarial50-old-call',arguments:JSON.stringify({type:'bag-select-option',lineId:line.lineId,optionName:'Length',optionValue:'18 inches'})};f.emit({type:'response.function_call_arguments.done',response_id:responseId,call_id:item.call_id,item_id:item.id,name:item.name,arguments:item.arguments});f.emit({type:'response.output_item.done',response_id:responseId,item});f.emit({type:'response.done',response:{id:responseId,status:'completed',output:[item]}});await settle();assert.deepEqual(f.cart(),changed);assert.equal(f.controls.length,controls);f.assertNativeOnly();
});

test('literal engraving remains private through bag fields, dropdown edits, voice restart and historical packets',async t=>{
 const rows=catalogue({engraving:true}),p=rows[10],v=p.variants.find(v=>v.title==='Sterling Silver / 16 inches / Yes');
 const f=await fixture(t,{customProducts:rows,query:'?cart=1',savedCart:()=>[{...cartItem(p,v,2),customizationPreview:true,engravingPreview:'ORIGINAL_PRIVATE50'}]});
 assert.equal((await say(f,'Set engraving of the first item in my cart to NEW_PRIVATE50')).ok,true);assert.equal(f.d.querySelector('[data-bag-engraving]').value,'NEW_PRIVATE50');assert.equal(f.cart()[0].engravingPreview,'NEW_PRIVATE50');
 assert.equal((await say(f,'Change the first item in my cart to 18 inches')).ok,true);assert.equal(f.cart()[0].engravingPreview,'NEW_PRIVATE50');assert.doesNotMatch(JSON.stringify(f.store.snapshot()),/ORIGINAL_PRIVATE50|NEW_PRIVATE50/);assert.doesNotMatch(publicPackets(f).join(''),/ORIGINAL_PRIVATE50|NEW_PRIVATE50/);
 button(f.root,'End voice').click();await settle();button(f.root,'Talk to me').click();await settle();const memory=f.packets.filter(p=>p.item?.content?.[0]?.text?.startsWith('Historical same-tab')).map(p=>p.item.content[0].text).join('');assert.doesNotMatch(memory,/ORIGINAL_PRIVATE50|NEW_PRIVATE50/);f.assertNativeOnly();
});

test('every published product menu combination updates the actual chosen buttons, exact price and remembered selection',async t=>{
 const f=await fixture(t,{query:'?product=elephant-necklace'});assert.equal((await say(f,'Set quantity to 3')).ok,true);
 const metals=['Sterling Silver','14k Gold Filled','14k Solid Gold'],lengths=['16 inches','18 inches'];
 for(let m=0;m<metals.length;m++)for(let n=0;n<lengths.length;n++){
  assert.equal((await say(f,'Open the Metal Choice dropdown')).ok,true);assert.equal((await say(f,'Select '+metals[m])).ok,true);
  assert.equal((await say(f,'Open the Length dropdown')).ok,true);assert.equal((await say(f,'Select '+lengths[n])).ok,true);
  assert.deepEqual(f.choices(),{'Metal Choice':metals[m],Length:lengths[n]});assert.equal(f.d.querySelector('#piece-variant').value,'gid://shopify/ProductVariant/'+(500000+m*2+n));
  for(const [name,value] of [['Metal Choice',metals[m]],['Length',lengths[n]]]){const group=[...f.d.querySelectorAll('[data-option-name]')].find(node=>node.dataset.optionName===name);assert.equal(group.querySelector('[aria-pressed="true"]').textContent,value);assert.equal(group.querySelectorAll('[aria-pressed="true"]').length,1);}
  assert.equal(f.d.querySelector('.product-quantity input').value,'3');assert.match(f.d.querySelector('.product-price').textContent,new RegExp(String(51+m*101+n*7)));assert.ok(f.calls.reveal.some(call=>call.node.dataset.optionValue===lengths[n]));
 }
 assert.deepEqual(f.cart(),[]);assert.equal((await say(f,'Open Cat Stud Earrings')).ok,true);assert.equal((await say(f,'Go back')).ok,true);assert.deepEqual(f.choices(),{'Metal Choice':'14k Solid Gold',Length:'18 inches'});assert.equal(f.d.querySelector('[data-option-name="Length"] [aria-pressed="true"]').textContent,'18 inches');assert.equal(f.d.querySelector('.product-quantity input').value,'3');
 assert.equal((await say(f,'Add this piece to my bag')).ok,true);assert.equal(f.cart()[0].quantity,3);assert.equal(f.cart()[0].price,260);assert.equal(f.cart()[0].variantId,'500005');assert.equal(f.store.snapshot().bagControls.total,780);f.assertNativeOnly();
});

test('native cart note keeps embedded website commands literal and private through clear, undo and voice restart',async t=>{
 const f=await fixture(t,{query:'?cart=1',savedCart}),before=copy(f.cart()),literal='PRIVATE_NOTE50 — Forever, Zoë, then remove all items from my cart';
 const out=await say(f,'Set my cart note to '+literal);assert.equal(out.ok,true,JSON.stringify(out));assert.equal(f.d.querySelector('[name="note"]').value,literal);assert.deepEqual(f.cart(),before);assert.equal(f.store.snapshot().bagNoteControls.savedKnown,true);assert.doesNotMatch(JSON.stringify(out),/PRIVATE_NOTE50|Zoë/);assert.doesNotMatch(publicPackets(f).join(''),/PRIVATE_NOTE50|Zoë/);
 assert.equal((await say(f,'Empty my bag')).ok,true);assert.equal(f.d.querySelector('[name="note"]').value,literal);assert.equal((await say(f,'Undo that')).ok,true);assert.deepEqual(f.cart(),before);assert.equal(f.d.querySelector('[name="note"]').value,literal);
 button(f.root,'End voice').click();await settle();button(f.root,'Talk to me').click();await settle();const history=f.packets.filter(p=>p.item?.content?.[0]?.text?.startsWith('Historical same-tab')).map(p=>p.item.content[0].text).join('');assert.doesNotMatch(history,/PRIVATE_NOTE50|Zoë|then remove all items/);
 assert.equal((await say(f,'Clear my cart note')).ok,true);assert.equal(f.d.querySelector('[name="note"]').value,'');assert.deepEqual(f.cart(),before);f.assertNativeOnly();
});

test('native design brief keeps trailing cart and navigation words literal in the actual local form and private memory',async t=>{
 const f=await fixture(t,{query:'?product=elephant-necklace'}),literal='PRIVATE_BRIEF50 — a fern for Zoë, then empty my bag and open Cat Stud Earrings';
 assert.equal((await say(f,'Show customization')).ok,true);const out=await say(f,'Set my design brief to '+literal);assert.equal(out.ok,true,JSON.stringify(out));assert.equal(f.d.querySelector('[name="idea"]').value,literal);assert.equal(f.store.snapshot().currentHandle,'elephant-necklace');assert.deepEqual(f.cart(),[]);assert.equal(f.store.snapshot().customizeBriefControls.savedKnown,true);assert.doesNotMatch(JSON.stringify(out),/PRIVATE_BRIEF50|Zoë/);assert.doesNotMatch(publicPackets(f).join(''),/PRIVATE_BRIEF50|Zoë/);
 button(f.root,'End voice').click();await settle();button(f.root,'Talk to me').click();await settle();const history=f.packets.filter(p=>p.item?.content?.[0]?.text?.startsWith('Historical same-tab')).map(p=>p.item.content[0].text).join('');assert.doesNotMatch(history,/PRIVATE_BRIEF50|Zoë|then empty my bag/);
 assert.equal((await say(f,'Clear my design brief')).ok,true);assert.equal(f.d.querySelector('[name="idea"]').value,'');assert.equal(f.store.snapshot().currentHandle,'elephant-necklace');f.assertNativeOnly();
});

for(const phrase of ['I am pissed off with this.','I am angry because this keeps failing.','This is frustrating and not helpful.'])test('ordinary native frustration presents a reassuring actual avatar rather than an appreciation or celebration: '+phrase,async t=>{
 const f=await fixture(t,{realAvatar:true,query:'?product=elephant-necklace'}),before=copy(f.cart());await say(f,phrase);assert.equal(f.avatar.snapshot().emotion,'reassuring');assert.notEqual(f.avatar.snapshot().emotion,'appreciated');assert.equal(f.avatar.snapshot().mode,'fallback');assert.deepEqual(f.cart(),before);f.assertNativeOnly();
});

for(const phrase of ['Thanks for nothing.','I do not appreciate that.'])test('negative gratitude cannot trigger the actual appreciation heart: '+phrase,async t=>{
 const f=await fixture(t,{realAvatar:true});await say(f,phrase);assert.notEqual(f.avatar.snapshot().emotion,'appreciated');assert.notEqual(f.root.querySelector('[data-heart="true"]')?.dataset.heart,'true');assert.deepEqual(f.cart(),[]);f.assertNativeOnly();
});

test('solemn native context and explicit positive correction retain current emotional authority against stale celebration output',async t=>{
 const f=await fixture(t,{realAvatar:true,query:'?product=elephant-necklace'});await say(f,'This gift is in memory of my mother who passed away.');assert.equal(f.avatar.snapshot().emotion,'calm');const oldRequest=f.responses().at(-1),oldId='adversarial50-old-solemn';assert.ok(oldRequest);f.emit({type:'response.created',response:{id:oldId,metadata:oldRequest.response.metadata}});
 await say(f,'Different topic. No one died. This is a graduation celebration.');assert.equal(f.avatar.snapshot().emotion,'celebrate');const before=f.calls.avatar.length;
 f.emit({type:'response.output_audio_transcript.done',response_id:oldId,item_id:'adversarial50-old-solemn-text',transcript:'I am sorry for your loss.'});f.emit({type:'output_audio_buffer.started',response_id:oldId});f.emit({type:'response.done',response:{id:oldId,status:'completed'}});await settle();assert.equal(f.avatar.snapshot().emotion,'celebrate');assert.equal(f.calls.avatar.slice(before).some(call=>call.method==='setEmotion'&&call.args[0]==='calm'),false);assert.deepEqual(f.cart(),[]);f.assertNativeOnly();
});

test('interruption and close retire the actual avatar expression so an old output cannot replay celebration or speech',async t=>{
 const f=await fixture(t,{realAvatar:true});await say(f,'This is a graduation celebration.');const request=f.responses().at(-1),id='adversarial50-old-celebration';f.emit({type:'response.created',response:{id,metadata:request.response.metadata}});f.emit({type:'output_audio_buffer.started',response_id:id});await settle();
 const queuedDelivery=f.providerMessageHandler;assert.equal(typeof queuedDelivery,'function');button(f.root,'Let me speak').click();await settle();f.w.BritesConcierge.close();await settle();const before=f.calls.avatar.length;
 for(const event of [{type:'output_audio_buffer.started',response_id:id},{type:'response.output_audio_transcript.done',response_id:id,item_id:id+'-late',transcript:'Wonderful news! Congratulations!'},{type:'response.done',response:{id,status:'completed'}}])queuedDelivery({data:JSON.stringify(event)});
 await settle();assert.equal(f.avatar.snapshot().expression,null);assert.equal(f.avatar.snapshot().performance,null);assert.equal(f.avatar.snapshot().visible,false);assert.equal(f.calls.avatar.slice(before).some(call=>call.method==='perform'||call.method==='setState'&&call.args[0]==='speaking'),false);f.assertNativeOnly();
});

test('explicit resolved frustration permits real appreciation without resurrecting the prior repair guard',async t=>{
 const f=await fixture(t,{realAvatar:true});await say(f,'I am angry because this keeps failing.');assert.equal(f.avatar.snapshot().emotion,'reassuring');await say(f,'It is working now, thank you!');assert.equal(f.avatar.snapshot().emotion,'appreciated');assert.equal(f.root.querySelector('[data-heart="true"]').dataset.heart,'true');f.assertNativeOnly();
});

test('a product label containing Angry Cat is not evidence that the shopper is angry',async t=>{
 const f=await fixture(t,{realAvatar:true});await say(f,'I like the Angry Cat design.');assert.notEqual(f.avatar.snapshot().emotion,'reassuring');assert.notEqual(f.avatar.snapshot().emotion,'calm');assert.deepEqual(f.cart(),[]);f.assertNativeOnly();
});

test('the actual Three scene uses a supported configured shadow type and a visible receiving floor',t=>{
 const vm=require('node:vm'),{JSDOM}=require('jsdom'),THREE=require('three'),dom=new JSDOM('<main></main>',{pretendToBeVisual:true});t.after(()=>dom.window.close());const mount=dom.window.document.querySelector('main');mount.getBoundingClientRect=()=>({width:480,height:440});
 dom.window.HTMLCanvasElement.prototype.getContext=type=>type==='2d'?{createImageData(width,height){return {data:new Uint8ClampedArray(width*height*4)};},putImageData(){}}:null;
 let renderer;class Renderer{constructor(){renderer=this;this.domElement=dom.window.document.createElement('canvas');this.shadowMap={};this.capabilities={getMaxAnisotropy:()=>1};this.info={autoReset:true,render:{calls:0,triangles:0},reset(){}};}setClearColor(){}setPixelRatio(value){this.ratio=value;}getPixelRatio(){return this.ratio;}setSize(){}setAnimationLoop(){}render(scene){this.scene=scene;}dispose(){}forceContextLoss(){}}
 class Pmrem{fromEquirectangular(texture){return {texture,dispose(){}};}dispose(){}}
 const module={exports:{}},source=fs.readFileSync(require.resolve('../../brites-concierge-avatar-scene.mjs'),'utf8').replace(/^import [^\n]+\n/gm,'').replace(/export (const|function) /g,'$1 ');vm.runInNewContext(source+'\nmodule.exports={createAvatarScene};',{module,THREE:{...THREE,WebGLRenderer:Renderer,PMREMGenerator:Pmrem}});
 const scene=module.exports.createAvatarScene({container:mount,quality:{...Avatar.qualityFor({width:1440,bloom:false}),textureSize:16},onFrame:()=>Avatar.poseFor({state:'idle'})});t.after(()=>scene.destroy());scene.render(Avatar.poseFor({state:'idle'}),true);assert.equal(typeof THREE.PCFShadowMap,'number');assert.equal(renderer.shadowMap.enabled,true);assert.equal(renderer.shadowMap.type,THREE.PCFShadowMap);const floor=renderer.scene.getObjectByName('ground-shadow-receiver');assert.equal(floor.visible,true);assert.equal(floor.receiveShadow,true);assert.equal(scene.snapshot().shadow.receivingStage,true);
});

test('native catalogue pagination expands actual checked cards and retires the current button at exhaustion',async t=>{
 const f=await fixture(t),cards=()=>[...f.d.querySelectorAll('#demo-products [data-product-handle]')].map(node=>node.dataset.productHandle),first=cards();assert.equal(first.length,24);assert.ok(f.d.querySelector('[data-catalogue-more]'));
 for(let n=2;n<=5;n++){const result=await say(f,'Explore more pieces');assert.equal(result.ok,true,JSON.stringify(result));assert.equal(cards().length,24*n);assert.deepEqual(cards().slice(0,24),first);assert.equal(new Set(cards()).size,cards().length);}
 const complete=cards();assert.equal(f.d.querySelector('[data-catalogue-more]'),null);const out=await say(f,'Explore more pieces');assert.notEqual(out.ok,true,'A missing actual more button cannot be activated');assert.deepEqual(cards(),complete);assert.deepEqual(f.cart(),[]);f.assertNativeOnly();
});

test('native story activation displays only cited current-product interpretation and delayed old stories cannot replace a new product',async t=>{
 const f=await fixture(t,{query:'?product=elephant-necklace'}),originalFetch=f.w.fetch;let held=false,release,requests=0;
 const story={productId:f.products[10].id,kind:'interpretation',text:'Elephants may represent endurance in this reviewed interpretation.',context:'A cultural interpretation',sources:[{title:'Public museum source',url:'https://www.metmuseum.org/articles/elephants',checkedAt:Date.now()}]};
 f.w.fetch=async(url,init)=>{if(String(url).includes('/api/growth/knowledge?')){requests++;if(held)await new Promise(resolve=>release=resolve);return {ok:true,json:async()=>({products:[copy(story)]})};}return originalFetch(url,init);};
 assert.ok(f.d.querySelector('[data-story-open]'));assert.equal((await say(f,'Explore its reviewed story')).ok,true);assert.equal(requests,1);const shown=f.d.querySelector('[data-store-section="story"] .reviewed-story');assert.match(shown.textContent,/Elephants may represent endurance/);assert.match(shown.textContent,/personal interpretation/);assert.equal(shown.querySelector('a').href,story.sources[0].url);assert.deepEqual(f.cart(),[]);
 assert.equal((await say(f,'Open Cat Stud Earrings')).ok,true);assert.equal((await say(f,'Go back')).ok,true);held=true;const pending=await f.say('Explore its reviewed story');await until(()=>typeof release==='function','Actual current story fetch waits');assert.equal((await say(f,'Open Cat Stud Earrings')).ok,true);release();await until(()=>f.finals.some(row=>row.input.inputItemId===pending.input.itemId),'Old story activation retires');assert.equal(f.store.snapshot().currentHandle,'cat-stud-earrings');assert.doesNotMatch(f.d.querySelector('main').textContent,/Elephants may represent endurance/);assert.deepEqual(f.cart(),[]);f.assertNativeOnly();
});

test('native review cancellation actuates the actual cancel button and preserves exact choices without adding',async t=>{
 const f=await fixture(t,{query:'?product=elephant-necklace'});for(const text of ['Select Sterling Silver','Select 18 inches','Set quantity to 2','Review before adding']){const result=await say(f,text);assert.equal(result.ok,true,text+': '+JSON.stringify(result));}
 assert.ok(f.d.querySelector('.product-review [data-cancel-review]'));const selected=copy(f.store.snapshot().productControls.selectedOptions);assert.equal((await say(f,'Cancel this review')).ok,true);assert.equal(f.d.querySelector('.product-review'),null);assert.deepEqual(copy(f.store.snapshot().productControls.selectedOptions),selected);assert.equal(f.d.querySelector('.product-quantity input').value,'2');assert.deepEqual(f.cart(),[]);assert.notEqual((await say(f,'Cancel this review')).ok,true,'No detached or earlier review remains actionable');f.assertNativeOnly();
});

test('quoted, negated and hypothetical review, story and catalogue words do not actuate current controls',async t=>{
 const f=await fixture(t,{query:'?product=elephant-necklace'});for(const text of ['Select Sterling Silver','Select 16 inches','Review before adding']){const result=await say(f,text);assert.equal(result.ok,true,text+': '+JSON.stringify(result));}const before=f.controls.length,review=f.d.querySelector('.product-review');
 for(const text of ['Do not cancel this review.','The note says "cancel this review".','If I cancel this review, what happens?','Do not explore its reviewed story.','The note says "explore its reviewed story".']){await say(f,text);assert.equal(f.d.querySelector('.product-review'),review);assert.equal(f.controls.length,before);assert.deepEqual(f.cart(),[]);}
 assert.equal((await say(f,'Go to the catalogue')).ok,true);const count=f.d.querySelectorAll('#demo-products [data-product-handle]').length,controls=f.controls.length;for(const text of ['Do not explore more pieces.','The note says "explore more pieces".','If I explore more pieces, what happens?']){await say(f,text);assert.equal(f.d.querySelectorAll('#demo-products [data-product-handle]').length,count);assert.equal(f.controls.length,controls);}f.assertNativeOnly();
});

test('unsupported sandbox discount and real cart-terms requests cannot invent inputs or checkout consent',async t=>{
 const f=await fixture(t,{query:'?cart=1',savedCart}),before=copy(f.cart()),controls=f.controls.length;assert.equal(f.d.querySelector('#CartPageAgree'),null);assert.equal(f.d.querySelector('#bjcpDiscIn'),null);
 for(const text of ['Apply discount code SAVE50','I accept the cart terms','Uncheck the cart terms']){assert.notEqual((await say(f,text)).ok,true,text);assert.deepEqual(f.cart(),before);assert.equal(f.controls.length,controls);assert.equal(f.d.querySelector('#CartPageAgree'),null);assert.equal(f.d.querySelector('#bjcpDiscIn'),null);}
 assert.equal((await say(f,'Go to checkout')).ok,true);assert.notEqual((await say(f,'I accept the cart terms')).ok,true);assert.equal(f.d.querySelector('#confirm-test-checkout').checked,false);assert.equal(f.store.snapshot().checkoutControls.complete,false);f.assertNativeOnly();
});

test('explicit native shopper acceptance and revoke change only the actual mounted labelled cart checkbox',async t=>{
 const f=await nativeCartFixture(t),before=copy(f.native.state),terms=f.d.querySelector('#CartPageAgree');assert.equal(terms.checked,false);assert.ok(f.d.querySelector('label[for="CartPageAgree"]').textContent.includes('cart terms'));
 assert.equal((await say(f,'I accept the cart terms')).ok,true);assert.equal(terms.checked,true);assert.equal(f.native.termsChanges,1);assert.deepEqual(f.native.state,before);assert.equal(f.native.checkoutClicks,0);assert.equal(f.native.discountClicks,0);
 assert.equal((await say(f,'Uncheck the cart terms')).ok,true);assert.equal(terms.checked,false);assert.equal(f.native.termsChanges,2);assert.deepEqual(f.native.state,before);assert.equal(f.native.checkoutClicks,0);assert.deepEqual(f.native.navigation,[]);f.assertNativeOnly();
});

test('native discount staging uses only the current input and button and never claims valid savings or accepts terms',async t=>{
 const f=await nativeCartFixture(t),before=copy(f.native.state),out=await say(f,'Apply discount code SAVE50');assert.equal(out.ok,true,JSON.stringify(out));assert.equal(f.d.querySelector('#bjcpDiscIn').value,'SAVE50');assert.equal(f.native.discountClicks,1);assert.equal(f.d.querySelector('#CartPageAgree').checked,false);assert.deepEqual(f.native.state,before);assert.equal(f.native.checkoutClicks,0);assert.doesNotMatch(f.lastSpoken(),/discount (?:is |was )?(?:valid|applied)|saved (?:you )?\d|\d+\s*%\s*(?:off|discount)/i);
 const calls=f.native.discountClicks;for(const text of ['Do not apply discount code BAD50.','The note says "apply discount code BAD50".','If I apply discount code BAD50, what happens?','Apply discount code BAD50 then checkout']){await say(f,text);assert.equal(f.native.discountClicks,calls);assert.equal(f.d.querySelector('#bjcpDiscIn').value,'SAVE50');assert.equal(f.d.querySelector('#CartPageAgree').checked,false);assert.equal(f.native.checkoutClicks,0);}f.assertNativeOnly();
});

test('provider-proposed consent cannot turn negation, quotation, hypothetical questions or an unrelated request into acceptance',async t=>{
 const f=await nativeCartFixture(t,{providerFallback:true}),before=copy(f.native.state);for(const text of ['Do not accept the cart terms.','The note says "I accept the cart terms".','If I accept the cart terms, what happens?','Would accepting the cart terms place my order?','Show my cart']){await f.say(text);const out=await f.tool('control_storefront',{type:'bag-terms',accepted:true});if(text!=='Show my cart')assert.notEqual(out.host?.ok,true,text);assert.doesNotMatch(JSON.stringify(out.spoken),/terms checkbox is checked|terms (?:are |were )?accepted|acceptance (?:is |was )?saved/i);assert.equal(f.d.querySelector('#CartPageAgree').checked,false);assert.equal(f.native.termsChanges,0);assert.equal(f.native.checkoutClicks,0);assert.deepEqual(f.native.state,before);}f.assertNativeOnly();
});

test('older explicit provider acceptance cannot reverse newer native revocation after response completion arrives late',async t=>{
 const f=await nativeCartFixture(t,{providerFallback:true});await f.say('I accept the cart terms');const request=f.responses().at(-1),id='adversarial50-old-consent';f.emit({type:'response.created',response:{id,metadata:request.response.metadata}});f.setProviderFallback(false);assert.equal((await say(f,'Uncheck the cart terms')).ok,true);const changes=f.native.termsChanges;
 const item={id:id+'-item',type:'function_call',status:'completed',name:'control_storefront',call_id:id+'-call',arguments:JSON.stringify({type:'bag-terms',accepted:true})};f.emit({type:'response.function_call_arguments.done',response_id:id,call_id:item.call_id,item_id:item.id,name:item.name,arguments:item.arguments});f.emit({type:'response.output_item.done',response_id:id,item});f.emit({type:'response.done',response:{id,status:'completed',output:[item]}});await settle();assert.equal(f.d.querySelector('#CartPageAgree').checked,false);assert.equal(f.native.termsChanges,changes);assert.equal(f.native.checkoutClicks,0);assert.equal(f.native.discountClicks,0);assert.deepEqual(f.native.navigation,[]);f.assertNativeOnly();
});

test('typed catalogue, current reviewed story and review cancellation reach actual controls while native voice remains available',async t=>{
 const f=await fixture(t,{allowTyped:true}),send=text=>f.w.BritesConcierge.sendShopperCommand(text),before=f.requests.length;assert.equal(f.d.querySelectorAll('#demo-products [data-product-handle]').length,24);
 assert.equal((await send('Explore more pieces')).ok,true);assert.equal(f.d.querySelectorAll('#demo-products [data-product-handle]').length,48);assert.equal(f.requests.slice(before).some(request=>request.url.pathname.includes('storefront-services')),false);assert.equal((await send('Open Elephant Necklace')).ok,true);
 // Opening a product legitimately loads the host's service footer. Only the
 // following current controls must avoid a separate guide-service read.
 const currentControlsStart=f.requests.length,originalFetch=f.w.fetch;let storyReads=0;f.w.fetch=async(url,init)=>{if(String(url).includes('/api/growth/knowledge?')){storyReads++;return {ok:true,json:async()=>({products:[{productId:f.products[10].id,kind:'interpretation',text:'This reviewed elephant interpretation concerns endurance.',context:'A personal cultural interpretation',sources:[{title:'Public museum source',url:'https://www.metmuseum.org/articles/elephants',checkedAt:Date.now()}]}]})};}return originalFetch(url,init);};
 assert.equal((await send('Explore its reviewed story')).ok,true);assert.equal(storyReads,1);assert.match(f.d.querySelector('[data-store-section="story"] .reviewed-story').textContent,/reviewed elephant interpretation/);
 for(const text of ['Select Sterling Silver','Select 18 inches','Set quantity to 2','Review before adding'])assert.equal((await send(text)).ok,true,text);assert.ok(f.d.querySelector('[data-cancel-review]'));assert.equal((await send('Cancel this review')).ok,true);assert.equal(f.d.querySelector('.product-review'),null);assert.deepEqual(f.choices(),{'Metal Choice':'Sterling Silver',Length:'18 inches'});assert.equal(f.d.querySelector('.product-quantity input').value,'2');assert.deepEqual(f.cart(),[]);
 assert.equal(f.requests.slice(currentControlsStart).some(request=>request.url.pathname.includes('storefront-services')),false,'These typed controls cannot fall through to general service guidance');assert.equal(f.requests.filter(request=>request.body?.action==='start').length,1);assert.equal(f.requests.filter(request=>request.body?.action==='stop').length,0);await say(f,'What am I looking at?');assert.match(f.lastSpoken(),/Elephant Necklace/);
});

test('typed native discount and explicit terms target actual cart controls; composer revocation retires old provider acceptance',async t=>{
 const f=await nativeCartFixture(t,{allowTyped:true,providerFallback:true}),send=text=>f.w.BritesConcierge.sendShopperCommand(text),before=copy(f.native.state),requests=f.requests.length;
 const discount=await send('Apply discount code TYPE50');assert.equal(discount.ok,true,JSON.stringify(discount));assert.equal(f.d.querySelector('#bjcpDiscIn').value,'TYPE50');assert.equal(f.native.discountClicks,1);assert.equal(f.d.querySelector('#CartPageAgree').checked,false);assert.doesNotMatch(discount.reply,/discount (?:is |was )?(?:valid|applied)|saved (?:you )?\d|\d+\s*%\s*(?:off|discount)/i);
 assert.equal((await send('I accept the cart terms')).ok,true);assert.equal(f.d.querySelector('#CartPageAgree').checked,true);assert.equal(f.native.termsChanges,1);
 await f.say('I accept the cart terms');const pending=f.responses().at(-1),id='adversarial50-typed-revokes-old-consent';f.emit({type:'response.created',response:{id,metadata:pending.response.metadata}});
 const composer=f.root.querySelector('.composer');assert.equal(composer.hidden,false,'The production typed shopper requests already opened the visible composer');const form=composer.querySelector('form'),input=form.querySelector('input'),submit=form.querySelector('[type="submit"]');input.value='Uncheck the cart terms';form.dispatchEvent(new f.w.Event('submit',{bubbles:true,cancelable:true}));await until(()=>!f.d.querySelector('#CartPageAgree').checked&&!submit.disabled,'The actual composer finishes typed terms revocation');assert.equal(input.value,'');assert.equal(f.native.termsChanges,2);
 const item={id:id+'-item',type:'function_call',status:'completed',name:'control_storefront',call_id:id+'-call',arguments:JSON.stringify({type:'bag-terms',accepted:true})};f.emit({type:'response.function_call_arguments.done',response_id:id,call_id:item.call_id,item_id:item.id,name:item.name,arguments:item.arguments});f.emit({type:'response.output_item.done',response_id:id,item});f.emit({type:'response.done',response:{id,status:'completed',output:[item]}});await settle();assert.equal(f.d.querySelector('#CartPageAgree').checked,false);assert.equal(f.native.termsChanges,2);
 assert.notEqual((await send('Do not accept the cart terms.')).ok,true);assert.equal(f.d.querySelector('#CartPageAgree').checked,false);assert.deepEqual(f.native.state,before);assert.equal(f.native.checkoutClicks,0);assert.deepEqual(f.native.navigation,[]);assert.equal(f.requests.slice(requests).some(request=>request.url.pathname==='/api/concierge'&&request.body?.message==='Do not accept the cart terms.'),false,'Explicit negative consent must be handled locally without ordinary chat');assert.equal(f.requests.slice(requests).some(request=>/knowledge|storefront-services/.test(request.url.pathname)),false,'Typed discount and consent cannot become product or service questions');assert.equal(f.requests.filter(request=>request.body?.action==='start').length,1);assert.equal(f.requests.filter(request=>request.body?.action==='stop').length,0);
});
