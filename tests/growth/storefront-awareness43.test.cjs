'use strict';
// The actual portable bridge and native adapter, with exact synthetic public
// products and cart transport. No production theme, cart, order or provider is used.
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs');
const {JSDOM,VirtualConsole}=require('jsdom');
const bridge=require('../../brites-storefront-bridge.js'),adapter=require('../../brites-shopify-storefront-adapter.js');
const clone=structuredClone,settle=async()=>{await new Promise(setImmediate);await new Promise(setImmediate);};
const daisy={id:'gid://shopify/Product/43001',handle:'daisy-necklace',title:'Daisy Necklace'};
const butterfly={id:'gid://shopify/Product/43002',handle:'butterfly-earrings',title:'Butterfly Earrings'};
const silver={name:'Metal Choice',value:'Sterling Silver'},length={name:'Chain Length',value:'16 inch'};
const controls={handle:daisy.handle,productId:daisy.id,productTitle:daisy.title,variantId:'gid://shopify/ProductVariant/43101',quantity:1,optionsOpen:false,openedOption:null,optionGroups:[{name:'Metal Choice',values:['Sterling Silver','Gold Filled']},{name:'Chain Length',values:['16 inch','18 inch']}],selectedOptions:[silver,length],selectedVariant:{id:'gid://shopify/ProductVariant/43101',title:'Sterling Silver / 16 inch',currency:'USD',price:54,available:true,options:[silver,length]},reviewReady:false,selectionStatus:'ready'};
const context={controlVersion:1,contextRevision:43,pageKind:'product',currentHandle:daisy.handle,focusedHandle:butterfly.handle,visiblePieces:[daisy,butterfly],loadedPieces:[daisy,butterfly],productControls:controls,bagControls:{lines:[],itemCount:2},navigationControls:{canGoBack:true,canGoForward:false},galleryControls:{handle:daisy.handle,imageCount:3,selectedIndex:1},changeControls:{canUndo:true,kind:'select-option'}};

function rawProduct(){return {id:43001,handle:daisy.handle,title:daisy.title,type:'Necklace',description:'<p>The charm is 12 mm wide and 1 mm thick.</p><script>PRIVATE_SCRIPT</script>',options:['Metal Choice','Chain Length'],variants:[['Sterling Silver','16 inch'],['Gold Filled','16 inch'],['Sterling Silver','18 inch'],['Gold Filled','18 inch']].map(([metal,len],index)=>({id:43101+index,title:metal+' / '+len,price:5400+index*500,available:true,options:[metal,len]}))};}
function native(t,options={}){
  const vc=new VirtualConsole(),errors=[];vc.on('jsdomError',e=>errors.push(e));
  const dom=new JSDOM(`<main id="MainContent"><div id="shopify-section-product-template"><h1 class="pi__title">Daisy Necklace</h1><span id="bjPrice">$54</span><div id="bjMedia">${[1,2,3].map(n=>'<figure class="m"><img src="https://britesjewelry.com/cdn/shop/products/daisy-'+n+'.jpg"></figure>').join('')}<div id="mDots"><i class="on"></i><i></i><i></i></div></div><form id="bjForm"><div id="bjMetals" data-idx="1"><button type="button" class="on" data-vi="0">Sterling Silver</button><button type="button" data-vi="1">Gold Filled</button></div><div class="bjselx"><select class="bjOptSel" data-idx="2"><option>16 inch</option><option>18 inch</option></select><button type="button" class="bjselx__btn" aria-haspopup="listbox" aria-expanded="false">16 inch</button></div><button type="button" data-q="-1">–</button><input id="bjQty" value="1" readonly><button type="button" data-q="1">+</button></form><details><summary>Product Details</summary><div class="bd">The charm is 12 mm wide and 1 mm thick.</div></details></div><a class="bjc-pcard" href="/products/butterfly-earrings"><span class="bjc-t">Butterfly Earrings</span></a></main>`,{url:'https://britesjewelry.com/products/daisy-necklace',runScripts:'outside-only',pretendToBeVisual:true,virtualConsole:vc});
  const win=dom.window,doc=win.document,raw=rawProduct(),requests=[],hooks=[],events=[],cart={currency:'USD',item_count:0,items:[]};
  win.HTMLElement.prototype.scrollIntoView=function(){if(this.tagName==='FIGURE'&&!options.brokenGallery){const index=[...doc.querySelectorAll('#bjMedia figure')].indexOf(this);doc.querySelectorAll('#mDots i').forEach((dot,n)=>dot.classList.toggle('on',index===n));}};
  doc.querySelectorAll('#bjMetals button').forEach(button=>button.onclick=()=>doc.querySelectorAll('#bjMetals button').forEach(other=>other.classList.toggle('on',other===button)));
  doc.querySelector('.bjselx__btn').onclick=e=>{if(!options.brokenMenu)e.currentTarget.setAttribute('aria-expanded',e.currentTarget.getAttribute('aria-expanded')==='true'?'false':'true');};
  doc.querySelectorAll('button[data-q]').forEach(button=>button.onclick=()=>doc.querySelector('#bjQty').value=String(Math.max(1,Number(doc.querySelector('#bjQty').value)+Number(button.dataset.q))));
  function addLine(variantId,quantity,properties){const variant=raw.variants.find(v=>String(v.id)===String(variantId));cart.items.push({key:variant.id+':fixture_'+cart.items.length,product_id:raw.id,variant_id:variant.id,product_title:raw.title,title:raw.title,variant_title:variant.title,quantity,price:variant.price,properties:properties||{}});cart.item_count+=quantity;}
  const fetch=async(value,init={})=>{const url=new URL(String(value),win.location.href),body=init.body?JSON.parse(init.body):null;requests.push({url,init,body});
    if(url.pathname.endsWith('/cart/add.js')){assert.equal(init.method,'POST');for(const item of body.items)addLine(item.id,item.quantity,item.properties);return {ok:true,json:async()=>({items:clone(cart.items)})};}
    if(url.pathname.endsWith('/cart.js'))return {ok:true,json:async()=>clone(cart)};
    if(url.pathname.endsWith('/products/daisy-necklace.js'))return {ok:true,json:async()=>clone(raw)};
    if(url.pathname==='/api/growth/product')return {ok:true,json:async()=>({live:true,product:instance.getInventory()[0]})};
    if(url.pathname==='/api/growth/catalogue')return {ok:true,json:async()=>({live:true,products:[instance.getInventory()[0]],pageInfo:{hasNextPage:false}})};
    if(url.pathname==='/api/concierge'&&typeof body?.event==='string')return {ok:true,json:async()=>({ok:true})};
    throw Error('Unexpected route '+url.pathname);
  };win.fetch=fetch;
  const add=options.add?async args=>{hooks.push(args);if(options.hookMutates!==false)addLine(args.variantId.split('/').pop(),args.quantity);if(options.wrongVariant){cart.items[0].variant_id=43102;}return options.add(args,cart);}:undefined;
  const instance=adapter.create({window:win,root:'/',theme:'brites-v1',currency:'USD',product:raw,variantCount:4,fetch,add});
  doc.addEventListener('brites-storefront:context',e=>events.push(clone(e.detail)));win.BritesStorefrontAdapter=instance;
  t.after(()=>{try{win.BritesConcierge?.close();}catch{}instance.destroy();win.close();});
  return {win,doc,raw,instance,requests,hooks,events,cart,errors,addLine};
}
const run=(f,action,id='current-shopper-43',extra={})=>f.instance.execute(action,{requestId:id,...extra});

for(const text of ['Tell me about what I am looking at','How big is the charm on this?','Tell me some interesting tidbits about this piece','What is the weight of the one I have opened?'])test('current detail question stays on the open product despite another hover: '+text,()=>assert.equal(bridge.resolveKnowledgeTarget(text,context).handle,daisy.handle));
test('only explicit pointer wording overrides the open product subject',()=>{assert.equal(bridge.resolveKnowledgeTarget('How big is the piece under my cursor?',context).handle,butterfly.handle);assert.equal(bridge.resolveKnowledgeTarget('Tell me about what I am looking at',{...context,pageKind:'collection'}).handle,butterfly.handle);assert.equal(bridge.resolveKnowledgeTarget('What is the size of Hidden Pendant?',context).handle,undefined);});
for(const text of ['Choose Gold Filled for Butterfly Earrings','Use Sterling Silver for the other necklace','Select Gold Filled for Hidden Pendant','Open the options for Butterfly Earrings','Add Butterfly Earrings to my cart','Add Hidden Pendant to my bag','Set quantity to 3 for Butterfly Earrings'])test('a different or unresolved target cannot modify the current product: '+text,()=>{const original=clone(context);assert.equal(bridge.resolve(text,context).ok,false);assert.deepEqual(context,original);});
for(const text of ['No, choose Gold Filled instead','Actually, select Gold Filled','Correction, choose Gold Filled'])test('explicit repair keeps the current exact product: '+text,()=>assert.deepEqual(bridge.resolve(text,context).action,{type:'select-option',handle:daisy.handle,optionName:'Metal Choice',optionValue:'Gold Filled'}));
for(const text of ['No','Stop','No, do not choose Gold Filled','No, choose Platinum instead','No, choose Gold Filled for Butterfly Earrings instead'])test('negative, unknown or cross-target repair stays refused: '+text,()=>assert.equal(bridge.resolve(text,context).ok,false));
for(const text of ['Help me choose this','Help me pick','Walk me through the options','Guide me through the choices','What should I choose?','What should I pick?'])test('guidance reveals the next actual missing choice without choosing it: '+text,()=>{const page={...clone(context),productControls:{...clone(controls),variantId:null,selectionStatus:'choosing',selectedOptions:[silver],nextOptionName:'Chain Length'}};assert.deepEqual(bridge.resolve(text,page).action,{type:'options',handle:daisy.handle,optionName:'Chain Length'});assert.deepEqual(page.productControls.selectedOptions,[silver]);});
test('guidance for another exact product does not open the current menu',()=>assert.equal(bridge.resolve('Walk me through the options for Butterfly Earrings',context).ok,false));

for(const text of ['Use gold filled instead','Make this gold filled','I would like the gold filled option'])test('explicit construction correction resolves one exact current published material with the menu closed: '+text,()=>{
  const page=clone(context);page.productControls.optionGroups[0].values=['Sterling Silver','14/20 Gold Filled'];
  assert.deepEqual(bridge.resolve(text,page).action,{type:'select-option',handle:daisy.handle,optionName:'Metal Choice',optionValue:'14/20 Gold Filled'});
});
test('ambiguous material aliases cannot silently choose an unrelated value or menu',()=>{
  const page=clone(context);page.productControls.optionGroups[0].values=['Sterling Silver','Solid Silver','14k Gold Filled','18k Gold Filled'];
  for(const text of ['Choose Silver','Use Gold','Use gold filled instead','Make this gold filled','Gold filled','18'])assert.equal(bridge.resolve(text,page).ok,false,text);
  page.productControls.optionsOpen=true;page.productControls.openedOption='Chain Length';assert.equal(bridge.resolve('Gold filled please',page).ok,false);
  page.productControls.openedOption='Metal Choice';assert.equal(bridge.resolve('Gold filled please',page).ok,false,'two constructions require an exact karat label');
});
test('an explicit unique material alias resolves the published value while a bare answer still requires its visible named menu',()=>{
  const page=clone(context);assert.deepEqual(bridge.resolve('Choose Silver',page).action,{type:'select-option',handle:daisy.handle,optionName:'Metal Choice',optionValue:'Sterling Silver'});
  assert.equal(bridge.resolve('Silver',page).ok,false);assert.equal(bridge.resolve('Silver please',page).ok,false);
  page.productControls.optionsOpen=true;page.productControls.openedOption='Metal Choice';assert.deepEqual(bridge.resolve('Silver please',page).action,{type:'select-option',handle:daisy.handle,optionName:'Metal Choice',optionValue:'Sterling Silver'});
});
test('literal numeric length boundaries preserve signs and decimal precision while slash and hyphen metal fineness stays literal',()=>{
  const page=clone(context);
  for(const text of ['Select -18 inch','Select +18 inch','Select −18 inch','Select .18 inch','Select +.18 inch','Select 18.0 inch','Select 18.5 inch','Select 14-18 inch','Select 18'])assert.equal(bridge.resolve(text,page).ok,false,text);
  assert.equal(bridge.resolve('Select 18 inch',page).action.optionValue,'18 inch');
  page.productControls.optionGroups[0].values=['Sterling Silver','14/20 Gold Filled','14-20 Gold Filled'];
  for(const value of ['14/20 Gold Filled','14-20 Gold Filled'])assert.equal(bridge.resolve('Select '+value,page).action.optionValue,value);
});
test('browser global and Node share natural discovery while facts and stop/correction words retain their own authority',t=>{
  const dom=new JSDOM('',{runScripts:'outside-only'});t.after(()=>dom.window.close());dom.window.eval(fs.readFileSync(require.resolve('../../brites-catalogue-intents.js'),'utf8'));dom.window.eval(fs.readFileSync(require.resolve('../../brites-storefront-bridge.js'),'utf8'));
  for(const text of ['Do you have butterfly earrings?','What animal earrings do you have?','No I meant butterfly earrings instead','What kinds of jewelry do you sell?','What types of jewellery do you have?','Could you show me your jewellery?']){
    const node=bridge.resolve(text,context),browser=dom.window.BritesStorefrontBridge.resolve(text,context);assert.equal(node.ok,true,text);assert.deepEqual(clone(browser.action),node.action,text);
  }
  for(const text of ['What materials are available?','Do they come in silver?','Wait, show butterfly earrings','Stop, actually animal earrings','No I meant do not search butterfly earrings instead','No I mean use gold filled for the other earrings','Show butterfly earrings and then add them to my bag'])assert.equal(bridge.resolve(text,context).ok,false,text);
  assert.equal(bridge.resolveKnowledgeTarget('Do they come in silver?',context).handle,daisy.handle);
});
test('a collection material or budget follow-up refines its known criteria while product questions retain current knowledge scope',()=>{
  const page={...clone(context),pageKind:'collection',currentHandle:'',focusedHandle:'',productControls:null,search:'animal earrings under 60 USD',filter:'earrings'},before=clone(page);
  assert.deepEqual(bridge.resolve('What about gold filled?',page).action,{type:'search',query:'animal earrings gold filled under 60 USD',filter:'earrings'});
  assert.deepEqual(bridge.resolve('What about under 40 USD?',page).action,{type:'search',query:'animal earrings under 40 USD',filter:'earrings'});
  assert.deepEqual(page,before);
  assert.equal(bridge.resolve('What materials are available?',page).delegated,'knowledge');
  const product={...clone(context),search:page.search,filter:page.filter},facts=bridge.resolve('What about gold filled?',product);
  assert.equal(facts.ok,false);assert.equal(facts.delegated,'knowledge');assert.equal(facts.targetHandle,daisy.handle);
  assert.equal(bridge.resolve('What about gold filled?',{...page,pageKind:'bag'}).ok,false);
  assert.equal(bridge.resolve('What about gold filled?',{...page,search:'',filter:'all'}).ok,false);
});
test('direct addition preserves unresolved choices so the host can offer its next step',()=>{const unresolved={...clone(context),productControls:{...clone(controls),variantId:null,selectedOptions:[silver],selectionStatus:'choosing'}};assert.deepEqual(bridge.resolve('Add this piece to my cart',unresolved).action,{type:'add',handle:daisy.handle});assert.deepEqual(bridge.resolve('Add this piece to my cart',context).action,{type:'add',handle:daisy.handle,variantId:controls.variantId});const legacy={...clone(context),productControls:{...clone(controls)}};delete legacy.productControls.selectionStatus;assert.equal(bridge.resolve('Add this piece to my cart',legacy).action.type,'review-add');});
test('cart count is known on the product page while unmounted lines remain partial',()=>{const snap=bridge.sanitizeSnapshot(context);assert.equal(snap.bagControls.itemCount,2);assert.equal(snap.bagControls.linesComplete,false);assert.deepEqual(snap.bagControls.lines,[]);});
test('current public product summary is exact, bounded and excludes private fields',t=>{const f=native(t),snap=bridge.sanitizeSnapshot(f.instance.snapshot());assert.equal(snap.currentProduct.handle,daisy.handle);assert.equal(snap.currentProduct.description,'The charm is 12 mm wide and 1 mm thick.');assert.doesNotMatch(JSON.stringify(snap),/PRIVATE|properties|token|customer/);assert.equal(bridge.sanitizeSnapshot({...f.instance.snapshot(),currentProduct:{...snap.currentProduct,handle:butterfly.handle}}).currentProduct,undefined);});
test('native shopper clicks publish latest options, quantity and visible dropdown on the canonical event',async t=>{const f=native(t),before=f.instance.snapshot();f.doc.querySelectorAll('#bjMetals button')[1].click();f.doc.querySelector('button[data-q="1"]').click();f.doc.querySelector('.bjselx__btn').click();await settle();assert(f.events.length);const latest=f.events.at(-1);assert.equal(latest.productControls.selectedVariant.id,'gid://shopify/ProductVariant/43102');assert.equal(latest.productControls.quantity,2);assert.equal(latest.productControls.optionsOpen,true);assert.equal(latest.productControls.openedOption,'Chain Length');assert(latest.contextRevision>before.contextRevision);assert.equal(f.requests.length,0);assert.equal(f.instance.snapshot().contextRevision,latest.contextRevision);});
test('gallery changes and manual native dropdown close update awareness without fetch',async t=>{const f=native(t);await run(f,{type:'options',handle:daisy.handle,optionName:'Chain Length'});f.doc.querySelector('.bjselx__btn').click();f.doc.querySelectorAll('#mDots i').forEach((dot,n)=>dot.classList.toggle('on',n===2));await settle();const latest=f.events.at(-1);assert.equal(latest.galleryControls.selectedIndex,3);assert.equal(latest.productControls.optionsOpen,false);assert.equal(f.requests.length,0);});
test('pointer release clears attention while retaining the open product',t=>{const f=native(t),card=f.doc.querySelector('a.bjc-pcard');card.dispatchEvent(new f.win.Event('pointerover',{bubbles:true}));assert.equal(f.instance.snapshot().focusedHandle,butterfly.handle);card.dispatchEvent(new f.win.MouseEvent('pointerout',{bubbles:true,relatedTarget:f.doc.querySelector('#bjQty')}));assert.equal(f.instance.snapshot().focusedHandle,'');assert.equal(f.instance.snapshot().currentHandle,daisy.handle);});
for(const broken of ['Menu','Gallery'])test('failed visible native '+broken.toLowerCase()+' postcondition never reports success',async t=>{const f=native(t,{['broken'+broken]:true}),action=broken==='Menu'?{type:'options',handle:daisy.handle,optionName:'Chain Length'}:{type:'gallery',handle:daisy.handle,index:3},result=await run(f,action);assert.equal(result.ok,false);assert.match(result.reason,/not confirmed/);});
test('repeated native image/option/quantity changes and undo preserve actual current controls',async t=>{const f=native(t);for(let cycle=0;cycle<4;cycle++){assert.equal((await run(f,{type:'gallery',handle:daisy.handle,index:3},'gallery-'+cycle)).ok,true);assert.equal((await run(f,{type:'undo'},'undo-gallery-'+cycle)).ok,true);assert.equal(f.instance.snapshot().galleryControls.selectedIndex,1);assert.equal((await run(f,{type:'select-option',handle:daisy.handle,optionName:'Metal Choice',optionValue:'Gold Filled'},'select-'+cycle)).ok,true);assert.equal((await run(f,{type:'undo'},'undo-option-'+cycle)).ok,true);assert.equal(f.instance.snapshot().productControls.selectedVariant.id,controls.variantId);assert.equal((await run(f,{type:'product-quantity',handle:daisy.handle,quantity:2},'quantity-'+cycle)).ok,true);assert.equal((await run(f,{type:'undo'},'undo-quantity-'+cycle)).ok,true);assert.equal(f.instance.snapshot().productControls.quantity,1);}assert.equal(f.requests.length,0);});
test('same-revision current page and gallery drift reject queued actions before dispatch',async()=>{for(const drift of [{currentHandle:butterfly.handle},{galleryControls:{...context.galleryControls,selectedIndex:2}}]){let page=clone(context),calls=0;const host={capabilities:{controlVersion:1,mode:'sandbox',actions:['gallery']},snapshot:()=>page,execute:async()=>{calls++;return {ok:true};}},api=bridge.create({storefront:host}),request=api.resolve('Show the next image');page={...page,...drift};assert.equal((await api.execute(request.action)).stale,true);assert.equal(calls,0);}});
test('explicit review remains supported when direct add is the default workflow',async()=>{const host={capabilities:{controlVersion:1,mode:'sandbox',actions:['add','review-add'],reviewBeforeAdd:false},snapshot:()=>clone(context),execute:async()=>({ok:true,prepared:true})},api=bridge.create({storefront:host}),request=api.resolve('Review adding this piece to my cart');assert.equal(request.action.type,'review-add');assert.equal((await api.execute(request.action)).ok,true);});
test('native direct add appears only with a real hook and checks the exact product and cart readback',async t=>{const legacy=native(t);assert(!legacy.instance.capabilities.actions.includes('add'));assert.equal(legacy.instance.snapshot().productControls.selectionStatus,undefined);const f=native(t,{add:async()=>({ok:true,cartChanged:true})}),result=await run(f,{type:'add',handle:daisy.handle,variantId:controls.variantId},'current-exact-add-43',{reviewAuthority:{}});assert.equal(result.ok,true,result.reason);assert.equal(result.cartChanged,true);assert.equal(result.snapshot.bagControls.itemCount,1);assert.equal(f.hooks.length,1);assert.deepEqual(f.requests.map(r=>r.url.pathname),['/cart.js','/products/daisy-necklace.js','/cart.js']);assert.equal((await run(f,{type:'add',handle:daisy.handle,variantId:controls.variantId},'current-exact-add-43',{reviewAuthority:{}})).suppressed,true);assert.equal(f.cart.item_count,1);});
for(const damage of ['price drift','false success','wrong variant','missing authority'])test('native direct add refuses '+damage+' without a false confirmed receipt',async t=>{const f=native(t,{add:async()=>({ok:true,cartChanged:true}),hookMutates:damage!=='false success',wrongVariant:damage==='wrong variant'});if(damage==='price drift')f.raw.variants[0].price+=100;const result=await run(f,{type:'add',handle:daisy.handle,variantId:controls.variantId},'exact-refusal-'+damage,damage==='missing authority'?{}:{reviewAuthority:{}});assert.equal(result.ok,false);assert.equal(result.cartChanged,undefined);if(damage==='price drift'||damage==='missing authority'){assert.equal(f.hooks.length,0);assert.equal(f.cart.item_count,0);}else assert.equal(f.hooks.length,1);});

test('native cart count remains unknown until its exact checked cart read',async t=>{
  const f=native(t),initial=f.instance.snapshot();
  assert.equal(initial.bagControls.countKnown,false);assert.equal(initial.bagControls.linesComplete,false);
  assert.equal(bridge.sanitizeSnapshot(initial).bagControls.countKnown,false);assert.equal(bridge.sanitizeSnapshot(initial).bagControls.linesComplete,false);
  await f.instance.refresh();const checked=f.instance.snapshot();
  assert.equal(checked.bagControls.countKnown,true);assert.equal(checked.bagControls.linesComplete,true);assert.equal(checked.bagControls.itemCount,0);
});

test('native partial product walkthrough opens the real missing choice and never selects another axis implicitly',async t=>{
  const f=native(t,{add:async()=>({ok:true,cartChanged:true})});
  f.doc.querySelectorAll('#bjMetals button').forEach(button=>button.classList.remove('on'));
  f.doc.querySelector('select.bjOptSel').selectedIndex=-1;
  assert.equal(f.instance.snapshot().productControls.selectionStatus,'choosing');
  let result=await run(f,{type:'add',handle:daisy.handle},'missing-choice-1');
  assert.equal(result.ok,false);assert.match(result.message,/Metal Choice.*Sterling Silver.*Gold Filled/);
  assert.equal(result.snapshot.productControls.openedOption,'Metal Choice');assert.equal(f.requests.length,0);assert.equal(f.hooks.length,0);
  result=await run(f,{type:'select-option',handle:daisy.handle,optionName:'Metal Choice',optionValue:'Gold Filled'},'first-choice');
  assert.equal(result.ok,true,result.reason);assert.equal(result.snapshot.productControls.variantId,null);
  assert.equal(f.doc.querySelector('select.bjOptSel').selectedIndex,-1,'the other axis stays unchosen');
  assert.equal(result.snapshot.productControls.nextOptionName,'Chain Length');assert.equal(result.snapshot.productControls.openedOption,'Chain Length');
  assert.equal(f.doc.querySelector('.bjselx__btn').getAttribute('aria-expanded'),'true');
  assert.equal(result.snapshot.changeControls.canUndo,false,'a native control with no published clear affordance cannot promise an unavailable undo');
  result=await run(f,{type:'add',handle:daisy.handle},'missing-choice-2');
  assert.equal(result.ok,false);assert.match(result.message,/Chain Length.*16 inch.*18 inch/);assert.equal(f.hooks.length,0);
  result=await run(f,{type:'select-option',handle:daisy.handle,optionName:'Chain Length',optionValue:'18 inch'},'second-choice');
  assert.equal(result.ok,true,result.reason);assert.equal(result.snapshot.productControls.selectionStatus,'ready');assert.equal(result.snapshot.productControls.variantId,'gid://shopify/ProductVariant/43104');
  assert.deepEqual(clone(result.snapshot.productControls.selectedOptions),[{name:'Metal Choice',value:'Gold Filled'},{name:'Chain Length',value:'18 inch'}]);
  result=await run(f,{type:'add',handle:daisy.handle,variantId:'gid://shopify/ProductVariant/43104'},'ready-add',{reviewAuthority:{}});
  assert.equal(result.ok,true,result.reason);assert.equal(result.cartChanged,true);assert.equal(f.hooks.length,1);assert.equal(f.hooks[0].variantId,'gid://shopify/ProductVariant/43104');assert.equal(f.cart.items[0].variant_id,43104);assert.equal(f.cart.item_count,1);
});

test('actual native adapter plus actual typed widget direct-add hook writes one exact current selection',async t=>{
  const f=native(t),script=f.doc.createElement('script');script.src='/brites-concierge.js';script.dataset.sandbox='false';Object.defineProperty(f.doc,'currentScript',{get:()=>script});
  f.win.BritesConciergeAvatar={create:()=>({setState(){},setEmotion(){},setVisible(){},setPaused(){},clearFocus(){},focusProduct(){},clearProduct(){},showProduct(){},cancelPerformance(){},setFloating(){},cue(){}})};
  f.win.eval(fs.readFileSync(require.resolve('../../brites-storefront-bridge.js'),'utf8'));f.win.eval(fs.readFileSync(require.resolve('../../brites-concierge.js'),'utf8'));
  assert.equal(typeof f.win.BritesConciergeShopifyControls.add,'function');
  f.win.BritesConcierge.open();const root=f.doc.querySelector('brites-concierge').shadowRoot;[...root.querySelectorAll('button')].find(b=>b.textContent.trim()==='Type instead').click();await settle();
  root.querySelector('.composer input').value='Add this piece to my cart';root.querySelector('form').dispatchEvent(new f.win.Event('submit',{bubbles:true,cancelable:true}));await settle();await settle();
  const writes=f.requests.filter(r=>r.url.pathname==='/cart/add.js');assert.equal(writes.length,1,root.querySelector('.caption-text')?.textContent);assert.equal(writes[0].body.items[0].id,43101);assert.equal(writes[0].body.items[0].quantity,1);assert.match(writes[0].body.items[0].properties._BritesConciergeRequest,/^[a-zA-Z0-9_-]{16,80}$/);assert.equal(f.cart.item_count,1);assert.equal(f.instance.snapshot().bagControls.itemCount,1);assert.match(root.querySelector('.caption-text').textContent,/in your bag|added/i);assert.equal(f.requests.filter(r=>r.url.pathname==='/api/concierge'&&typeof r.body?.message==='string').length,0);assert.equal(f.errors.length,0);
});
