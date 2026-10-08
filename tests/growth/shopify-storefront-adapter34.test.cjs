const test=require('node:test');
const assert=require('node:assert/strict');
const {JSDOM}=require('jsdom');
const fs=require('node:fs');
const path=require('node:path');
const adapter=require('../../brites-shopify-storefront-adapter.js');

function product(overrides={}){
  const variants=[];
  for(const metal of ['Sterling Silver','14k Gold Filled'])for(const length of ['14 inch','18 inch'])for(const engraving of ['None','Engraved']){
    const index=variants.length;
    variants.push({id:1001+index,title:[metal,length,engraving].join(' / '),price:5400+index*500,available:true,options:[metal,length,engraving]});
  }
  return {id:9001,handle:'sample-bunny',title:'Sample Bunny Necklace',type:'Necklace',options:[{name:'Metal Choice'},{name:'Necklace Length'},{name:'Engraving'}],variants,...overrides};
}
function fixture(t,{root='/',url,raw=product(),cart,reviewAdd,fetch,html,theme='brites-v1',currency='USD'}={}){
  const prefix=root;
  const markup=html===undefined?`<form action="${prefix}search" method="get"><input type="search" name="q"></form>
    <a href="${prefix}collections/all">All</a><a href="${prefix}collections/earrings">Earrings</a><a href="${prefix}pages/custom-studio">Design Studio</a>
    <main id="MainContent"><div id="shopify-section-product-template"><h1 class="pi__title">Sample Bunny Necklace</h1><span id="bjPrice">$54</span>
      <div id="bjMedia"><figure><img alt="Sample Bunny Necklace"></figure></div>
      <form id="bjForm"><div id="bjMetals" data-idx="1"><button type="button" data-vi="0" class="on">Sterling Silver</button><button type="button" data-vi="1">14k Gold Filled</button></div>
      <div class="bjselx"><select class="bjOptSel bjselx__native" data-idx="2"><option>14 inch</option><option>18 inch</option></select><button type="button" class="bjselx__btn" aria-haspopup="listbox" aria-expanded="false">14 inch</button></div>
      <div id="bjEngr" data-idx="3"><input type="checkbox" id="bjEngrChk"><input id="bjEngrTxt" value="PRIVATE-GIFT-NOTE"></div>
      <button type="button" data-q="-1">–</button><input id="bjQty" value="1" readonly><button type="button" data-q="1">+</button><button id="bjAdd" type="button">Add to Cart</button></form>
      <details open><summary>Product Details</summary><p>Published piece details.</p></details><details><summary>Materials &amp; Care</summary></details><details><summary>Shipping &amp; Returns</summary></details>
    </div><a class="bjc-pcard" href="${prefix}products/sample-earrings"><div class="bjc-t">Sample Earrings</div></a></main>`:html;
  const dom=new JSDOM(markup,{url:url||`https://britesjewelry.com${prefix}products/sample-bunny`});
  const win=dom.window,doc=win.document,calls=[],nav=[],reveals=[],reviews=[];
  let addClicks=0,quantityClicks=0,nativeQuantity=Number(doc.querySelector('#bjQty')?.value||1);
  win.HTMLElement.prototype.scrollIntoView=function(options){reveals.push({node:this,options});};
  doc.querySelectorAll('#bjMetals button').forEach(button=>button.addEventListener('click',()=>{doc.querySelectorAll('#bjMetals button').forEach(b=>b.classList.toggle('on',b===button));}));
  doc.querySelector('.bjselx__btn')?.addEventListener('click',event=>event.currentTarget.setAttribute('aria-expanded',event.currentTarget.getAttribute('aria-expanded')==='true'?'false':'true'));
  doc.querySelector('#bjAdd')?.addEventListener('click',()=>{addClicks++;});
  doc.querySelectorAll('button[data-q]').forEach(button=>button.addEventListener('click',()=>{quantityClicks++;nativeQuantity=Math.max(1,nativeQuantity+Number(button.getAttribute('data-q')));doc.querySelector('#bjQty').value=String(nativeQuantity);}));
  const state={raw,cart:cart||{currency,item_count:2,items:[{key:'1001:opaque',product_id:9001,variant_id:1001,quantity:2,properties:{giftNote:'PRIVATE-GIFT-NOTE'},customer:'PRIVATE-CUSTOMER'}],token:'PRIVATE-CART-TOKEN',note:'PRIVATE-GIFT-NOTE'}};
  const fakeFetch=fetch||async function(url,options){calls.push({url,options});return {ok:true,json:async()=>url.endsWith('cart.js')?structuredClone(state.cart):structuredClone(state.raw)};};
  const instance=adapter.create({window:win,root,theme,currency,product:raw,variantCount:raw?.variants.length,fetch:fakeFetch,navigate:value=>nav.push(value),reviewAdd:reviewAdd?async (args)=>{reviews.push(args);return reviewAdd(args);}:undefined});
  t.after(()=>{instance?.destroy();win.close();});
  return {instance,win,doc,calls,nav,reveals,reviews,state,addClicks:()=>addClicks,quantityClicks:()=>quantityClicks,nativeQuantity:()=>nativeQuantity};
}
const run=(f,action,requestId='request-one',extra={})=>f.instance.execute(action,{requestId,...extra});

test('matches the live custom form without exposing shopper notes or cart tokens',t=>{
  const f=fixture(t),snap=f.instance.snapshot();
  assert.deepEqual(snap.productControls,{handle:'sample-bunny',productId:'gid://shopify/Product/9001',productTitle:'Sample Bunny Necklace',productType:'Necklace',variantId:'gid://shopify/ProductVariant/1001',quantity:1,optionsOpen:false,openedOption:null,optionGroups:[{name:'Metal Choice',values:['Sterling Silver','14k Gold Filled']},{name:'Necklace Length',values:['14 inch','18 inch']},{name:'Engraving',values:['None','Engraved']}],selectedOptions:[{name:'Metal Choice',value:'Sterling Silver'},{name:'Necklace Length',value:'14 inch'},{name:'Engraving',value:'None'}],selectedVariant:{id:'gid://shopify/ProductVariant/1001',title:'Sterling Silver / 14 inch / None',price:54,currency:'USD',available:true,options:[{name:'Metal Choice',value:'Sterling Silver'},{name:'Necklace Length',value:'14 inch'},{name:'Engraving',value:'None'}]},itemTotalPrice:54,reviewReady:false});
  assert.equal(snap.checkoutControls,null);assert.deepEqual(snap.bagControls,{lines:[],itemCount:0,countKnown:false,linesComplete:false});
  assert.equal(JSON.stringify(snap).includes('PRIVATE'),false);
});
test('capabilities distinguish real controls from simulated checkout and unverified cart rows',t=>{
  const f=fixture(t),caps=f.instance.capabilities;
  assert.equal(caps.mode,'shopify');assert.equal(caps.finalOrder,false);assert.equal(caps.checkoutMode,'shopper-handoff');assert.equal(caps.reviewBeforeAdd,true);
  for(const action of ['options','select-option','product-quantity','search','filter','customize'])assert(caps.actions.includes(action));
  for(const action of ['checkout-complete','checkout-option','checkout-step','gift-preferences','bag-remove','bag-quantity','zoom','review-add'])assert(!caps.actions.includes(action));
});
test('missing or changed theme structure fails closed rather than substituting guessed controls',t=>{
  const f=fixture(t);f.doc.querySelector('#bjMetals button[data-vi="1"]').textContent='Gold';
  assert.equal(f.instance.snapshot().productControls,null);assert(!f.instance.capabilities.actions.includes('select-option'));
  f.doc.querySelector('#bjForm').id='other-form';assert(!f.instance.capabilities.actions.includes('options'));
});
test('duplicate selectors fail closed',t=>{
  const f=fixture(t);f.doc.querySelector('#MainContent').appendChild(f.doc.querySelector('#bjForm').cloneNode(true));
  assert.equal(f.instance.snapshot().productControls,null);assert(!f.instance.capabilities.actions.includes('product-quantity'));
});
test('shared context revisions change only with actual identity or selection changes',t=>{
  const f=fixture(t),first=f.instance.snapshot();assert.equal(f.instance.snapshot().contextRevision,first.contextRevision);
  f.doc.querySelector('#bjQty').value='2';assert(f.instance.snapshot().contextRevision>first.contextRevision);
  const second=f.instance.snapshot();assert.equal(f.instance.snapshot().contextRevision,second.contextRevision);
});
test('only current visible native product cards can be referenced',async t=>{
  const f=fixture(t);f.doc.querySelector('#MainContent').insertAdjacentHTML('beforeend','<a class="bjc-pcard" href="https://competitor.example/products/other"><div class="bjc-t">Other</div></a><a class="bjc-pcard" hidden href="/products/hidden"><div class="bjc-t">Hidden</div></a>');
  assert.deepEqual(f.instance.snapshot().visiblePieces.map(p=>p.handle),['sample-bunny','sample-earrings']);
  assert.equal((await run(f,{type:'open',handle:'other'})).ok,false);assert.deepEqual(f.nav,[]);
});
test('pointer attention uses mounted exact product identity and never reads note fields',t=>{
  const f=fixture(t);f.doc.querySelector('a.bjc-pcard').dispatchEvent(new f.win.Event('pointerover',{bubbles:true}));
  assert.equal(f.instance.snapshot().focusedHandle,'sample-earrings');
});
test('opens the verified existing native selector immediately without a product or cart GET',async t=>{
  const f=fixture(t,{root:'/en-ca/'}),res=await run(f,{type:'options',handle:'sample-bunny',optionName:'Necklace Length'});
  assert.equal(res.ok,true);assert.equal(f.doc.querySelector('.bjselx__btn').getAttribute('aria-expanded'),'true');
  assert.equal(res.snapshot.productControls.openedOption,'Necklace Length');assert.deepEqual(f.calls,[]);assert.equal(f.addClicks(),0);
});
test('literal partial option selection preserves every other group',async t=>{
  const f=fixture(t),res=await run(f,{type:'select-option',handle:'sample-bunny',optionName:'Necklace Length',optionValue:'18 inch'});
  assert.equal(res.ok,true);assert.equal(res.snapshot.productControls.variantId,'gid://shopify/ProductVariant/1003');
  assert.deepEqual(res.snapshot.productControls.selectedOptions,[{name:'Metal Choice',value:'Sterling Silver'},{name:'Necklace Length',value:'18 inch'},{name:'Engraving',value:'None'}]);assert.equal(f.addClicks(),0);
});
test('full variant selection updates all exact native groups together without adding',async t=>{
  const f=fixture(t),res=await run(f,{type:'select-option',handle:'sample-bunny',variantId:'gid://shopify/ProductVariant/1008'});
  assert.equal(res.ok,true);assert.equal(f.doc.querySelector('#bjMetals .on').textContent,'14k Gold Filled');assert.equal(f.doc.querySelector('select.bjOptSel').value,'18 inch');assert.equal(f.doc.querySelector('#bjEngrChk').checked,true);assert.equal(f.doc.querySelector('#bjEngrTxt').value,'PRIVATE-GIFT-NOTE');assert.equal(f.addClicks(),0);
});
for(const [name,action] of [
  ['unknown literal group',{type:'select-option',handle:'sample-bunny',optionName:'Length',optionValue:'18 inch'}],
  ['unknown literal value',{type:'select-option',handle:'sample-bunny',optionName:'Necklace Length',optionValue:'20 inch'}],
  ['unknown variant',{type:'select-option',handle:'sample-bunny',variantId:'gid://shopify/ProductVariant/9999'}]
])test(`rejects ${name} without changing native choices or reading the network`,async t=>{const f=fixture(t),res=await run(f,action);assert.equal(res.ok,false);assert.equal(f.doc.querySelector('select.bjOptSel').value,'14 inch');assert.deepEqual(f.calls,[]);assert.equal(f.addClicks(),0);});
for(const [name,change,action] of [
  ['unavailable exact combination',f=>f.state.raw.variants[2].available=false,{type:'select-option',handle:'sample-bunny',optionName:'Necklace Length',optionValue:'18 inch'}],
  ['selling-plan variant',f=>f.state.raw.variants[7].requires_selling_plan=true,{type:'select-option',handle:'sample-bunny',variantId:'gid://shopify/ProductVariant/1008'}]
])test(`a completed public refresh rejects ${name} before any local selection changes`,async t=>{
  const f=fixture(t);change(f);assert.equal(await f.instance.refresh(),true);assert.deepEqual(f.calls.map(c=>c.url),['/cart.js','/products/sample-bunny.js']);
  const res=await run(f,action);assert.equal(res.ok,false);assert.equal(f.doc.querySelector('select.bjOptSel').value,'14 inch');assert.equal(f.doc.querySelector('#bjMetals .on').textContent,'Sterling Silver');assert.equal(f.doc.querySelector('#bjEngrChk').checked,false);assert.equal(f.calls.length,2);assert.equal(f.addClicks(),0);
});
for(const [name,change] of [
  ['mismatched product identity',f=>f.state.raw={...f.state.raw,id:9002}],
  ['truncated variant response',f=>f.state.raw={...f.state.raw,variants:f.state.raw.variants.slice(0,4)}],
  ['newly unavailable current variant',f=>f.state.raw.variants[0].available=false],
  ['new selling-plan requirement',f=>f.state.raw.variants[0].requires_selling_plan=true]
])test(`fresh review refuses ${name} despite a locally loaded current choice`,async t=>{
  const authority={},f=fixture(t,{root:'/en-ca/',reviewAdd:async()=>({prepared:true})});change(f);
  const res=await run(f,{type:'review-add',handle:'sample-bunny',variantId:'gid://shopify/ProductVariant/1001'},'fresh-review',{reviewAuthority:authority});
  assert.equal(res.ok,false);assert.equal(f.reviews.length,0);assert.equal(f.doc.querySelector('select.bjOptSel').value,'14 inch');assert.equal(f.nativeQuantity(),1);assert.equal(f.addClicks(),0);assert.deepEqual(f.calls.map(c=>c.url),['/en-ca/cart.js','/en-ca/products/sample-bunny.js']);assert(f.calls.every(c=>c.options.method==='GET'&&c.options.cache==='no-store'&&c.options.credentials==='same-origin'));
});
test('does not report success if a theme ignores the option change',async t=>{
  const f=fixture(t);f.doc.querySelector('select').addEventListener('change',e=>{e.target.value='14 inch';});
  const res=await run(f,{type:'select-option',handle:'sample-bunny',optionName:'Necklace Length',optionValue:'18 inch'});assert.equal(res.ok,false);assert.match(res.reason,/could not confirm/);
});
test('a current product quantity updates its ordinary control without submitting',async t=>{
  const f=fixture(t),res=await run(f,{type:'product-quantity',handle:'sample-bunny',quantity:20});assert.equal(res.ok,true);assert.equal(f.doc.querySelector('#bjQty').value,'20');assert.equal(f.nativeQuantity(),20);assert.equal(f.quantityClicks(),19);assert.equal(f.addClicks(),0);
});
test('readonly native quantity decreases through existing step controls and an exact no-op makes no click',async t=>{
  const f=fixture(t);assert.equal((await run(f,{type:'product-quantity',handle:'sample-bunny',quantity:4},'four')).ok,true);assert.equal((await run(f,{type:'product-quantity',handle:'sample-bunny',quantity:2},'two')).ok,true);assert.equal(f.nativeQuantity(),2);assert.equal(f.quantityClicks(),5);
  assert.equal((await run(f,{type:'product-quantity',handle:'sample-bunny',quantity:2},'same')).ok,true);assert.equal(f.quantityClicks(),5);assert.equal(f.addClicks(),0);
});
test('an editable quantity fallback uses ordinary input and change events',async t=>{
  const f=fixture(t),input=f.doc.querySelector('#bjQty');input.readOnly=false;let inputCount=0,changeCount=0;input.addEventListener('input',()=>inputCount++);input.addEventListener('change',()=>changeCount++);
  assert.equal((await run(f,{type:'product-quantity',handle:'sample-bunny',quantity:3})).ok,true);assert.equal(input.value,'3');assert.equal(inputCount,1);assert.equal(changeCount,1);assert.equal(f.quantityClicks(),0);assert.equal(f.addClicks(),0);
});
test('readonly quantity without verified native step buttons has no mutation capability',async t=>{
  const f=fixture(t);f.doc.querySelector('button[data-q="1"]').remove();assert(!f.instance.capabilities.actions.includes('product-quantity'));assert.equal((await run(f,{type:'product-quantity',handle:'sample-bunny',quantity:2})).ok,false);assert.equal(f.nativeQuantity(),1);assert.equal(f.calls.length,0);
});
test('duplicate quantity step controls fail closed instead of selecting the first',t=>{
  const f=fixture(t),button=f.doc.querySelector('button[data-q="1"]');button.after(button.cloneNode(true));assert(!f.instance.capabilities.actions.includes('product-quantity'));
});
test('disabled native quantity step does not report a fabricated success',async t=>{
  const f=fixture(t);f.doc.querySelector('button[data-q="1"]').disabled=true;const res=await run(f,{type:'product-quantity',handle:'sample-bunny',quantity:2});assert.equal(res.ok,false);assert.equal(f.nativeQuantity(),1);assert.equal(f.quantityClicks(),0);
});
test('quantity stops when the real theme rejects a requested step',async t=>{
  const f=fixture(t);f.doc.querySelector('button[data-q="1"]').addEventListener('click',()=>{f.doc.querySelector('#bjQty').value='1';});const res=await run(f,{type:'product-quantity',handle:'sample-bunny',quantity:3});assert.equal(res.ok,false);assert.match(res.reason,/confirm the quantity step/);assert.equal(f.quantityClicks(),1);assert.equal(f.addClicks(),0);
});
for(const value of [0,-1,21,1.5,'2',null,Number.MAX_SAFE_INTEGER])test(`rejects invalid quantity ${String(value)}`,async t=>{const f=fixture(t),res=await run(f,{type:'product-quantity',handle:'sample-bunny',quantity:value});assert.equal(res.ok,false);assert.equal(f.doc.querySelector('#bjQty').value,'1');assert.equal(f.calls.length,0);});
test('review prepares only the exact current choice via opaque request-local widget authority',async t=>{
  const authority={},f=fixture(t,{reviewAdd:async()=>({prepared:true})});f.state.raw.variants[0].price=5900;
  const res=await run(f,{type:'review-add',handle:'sample-bunny',variantId:'gid://shopify/ProductVariant/1001'},'review-one',{reviewAuthority:authority});
  assert.equal(res.ok,true);assert.equal(res.cartChanged,false);assert.equal(res.requiredCustomerClick,'Confirm add to bag');assert.equal(f.reviews.length,1);assert.equal(f.reviews[0].reviewAuthority,authority);assert.equal(f.reviews[0].product.variants[0].price,59);assert.equal(f.reviews[0].quantity,1);assert.equal(f.addClicks(),0);assert(f.calls.every(c=>c.options.method==='GET'));
});
test('guessed request IDs cannot prepare a review without the opaque authority',async t=>{
  const f=fixture(t,{reviewAdd:async()=>({prepared:true})}),res=await run(f,{type:'review-add',handle:'sample-bunny'});assert.equal(res.ok,false);assert.equal(f.reviews.length,0);assert.equal(f.addClicks(),0);
});
test('review cannot silently select a different variant',async t=>{
  const f=fixture(t,{reviewAdd:async()=>({prepared:true})}),res=await run(f,{type:'review-add',handle:'sample-bunny',variantId:'gid://shopify/ProductVariant/1008'},'request-one',{reviewAuthority:{}});assert.equal(res.ok,false);assert.equal(f.reviews.length,0);
});
test('widget hold refusal is preserved as failed preparation',async t=>{
  const f=fixture(t,{reviewAdd:async()=>({ok:false,reason:'This piece needs its product details checked.'})}),res=await run(f,{type:'review-add',handle:'sample-bunny'},'request-one',{reviewAuthority:{}});assert.equal(res.ok,false);assert.match(res.reason,/details checked/);assert.equal(f.addClicks(),0);
});
test('duplicate review request cannot create multiple reviews',async t=>{
  const f=fixture(t,{reviewAdd:async()=>({prepared:true})}),action={type:'review-add',handle:'sample-bunny'},metadata={reviewAuthority:{}};
  assert.equal((await run(f,action,'same',metadata)).ok,true);assert.equal((await run(f,action,'same',metadata)).suppressed,true);assert.equal(f.reviews.length,1);
});
test('aborted request does not fetch or mutate',async t=>{
  const f=fixture(t),controller=new AbortController();controller.abort();const res=await run(f,{type:'product-quantity',handle:'sample-bunny',quantity:2},'request-one',{signal:controller.signal});assert.equal(res.cancelled,true);assert.equal(f.calls.length,0);
});
test('a shopper quantity change during fresh review reads cancels the stale review',async t=>{
  let release;const wait=new Promise(resolve=>release=resolve),raw=product();
  const f=fixture(t,{reviewAdd:async()=>({prepared:true}),fetch:async url=>{await wait;return {ok:true,json:async()=>url.endsWith('cart.js')?{currency:'USD',item_count:0,items:[]}:raw};}});
  const work=run(f,{type:'review-add',handle:'sample-bunny',variantId:'gid://shopify/ProductVariant/1001'},'fresh-review',{reviewAuthority:{}});f.doc.querySelector('button[data-q="1"]').click();release();const res=await work;assert.equal(res.stale,true);assert.equal(f.doc.querySelector('#bjQty').value,'2');assert.equal(f.nativeQuantity(),2);assert.equal(f.reviews.length,0);assert.equal(f.addClicks(),0);
});
test('newer local shopper request supersedes an earlier asynchronous fresh review',async t=>{
  let release;const wait=new Promise(resolve=>release=resolve),raw=product();
  const f=fixture(t,{reviewAdd:async()=>({prepared:true}),fetch:async url=>{await wait;return {ok:true,json:async()=>url.endsWith('cart.js')?{currency:'USD',item_count:0,items:[]}:raw};}});
  const older=run(f,{type:'review-add',handle:'sample-bunny',variantId:'gid://shopify/ProductVariant/1001'},'older',{reviewAuthority:{}});const newer=await run(f,{type:'highlight',handle:'sample-bunny',section:'price'},'newer');release();assert.equal(newer.ok,true);assert.equal((await older).cancelled,true);assert.equal(f.doc.querySelector('#bjQty').value,'1');assert.equal(f.reviews.length,0);assert.equal(f.addClicks(),0);
});
test('a failed fresh GET never authorizes a cart review',async t=>{
  const f=fixture(t,{reviewAdd:async()=>({prepared:true}),fetch:async()=>({ok:false,status:503})}),res=await run(f,{type:'review-add',handle:'sample-bunny',variantId:'gid://shopify/ProductVariant/1001'},'failed-review',{reviewAuthority:{}});assert.equal(res.ok,false);assert.equal(f.reviews.length,0);assert.equal(f.nativeQuantity(),1);assert.equal(f.addClicks(),0);
});
test('local reversible controls stay immediate even when a future public read would fail',async t=>{
  let attempted=0;const f=fixture(t,{fetch:async()=>{attempted++;return {ok:false,status:503};}});
  assert.equal((await run(f,{type:'options',handle:'sample-bunny',optionName:'Necklace Length'},'menu')).ok,true);
  assert.equal((await run(f,{type:'select-option',handle:'sample-bunny',optionName:'Necklace Length',optionValue:'18 inch'},'length')).ok,true);
  assert.equal((await run(f,{type:'product-quantity',handle:'sample-bunny',quantity:3},'quantity')).ok,true);
  assert.equal(attempted,0);assert.equal(f.doc.querySelector('select.bjOptSel').value,'18 inch');assert.equal(f.nativeQuantity(),3);assert.equal(f.addClicks(),0);
});
for(const [name,tamper] of [
  ['changed literal metal label',f=>f.doc.querySelector('#bjMetals button[data-vi="0"]').textContent='Gold'],
  ['changed native length option',f=>f.doc.querySelector('select.bjOptSel option').textContent='16 inch'],
  ['duplicated current product form',f=>f.doc.querySelector('#MainContent').appendChild(f.doc.querySelector('#bjForm').cloneNode(true))],
  ['changed product route',f=>f.win.history.pushState({},'','/products/another-piece')]
])test(`fresh cart review fails closed after ${name} during its read`,async t=>{
  let release;const wait=new Promise(resolve=>release=resolve),raw=product(),f=fixture(t,{reviewAdd:async()=>({prepared:true}),fetch:async url=>{await wait;return {ok:true,json:async()=>url.endsWith('cart.js')?{currency:'USD',item_count:0,items:[]}:raw};}});
  const work=run(f,{type:'review-add',handle:'sample-bunny',variantId:'gid://shopify/ProductVariant/1001'},'binding-review',{reviewAuthority:{}});tamper(f);release();const res=await work;assert.equal(res.ok,false);assert.equal(f.reviews.length,0);assert.equal(f.addClicks(),0);assert.equal(f.nativeQuantity(),1);
});
test('highlight is visible and safely restores the pre-existing outline on destroy',async t=>{
  const f=fixture(t),price=f.doc.querySelector('#bjPrice');price.style.outline='1px dashed red';price.style.outlineOffset='2px';
  assert.equal((await run(f,{type:'highlight',handle:'sample-bunny',section:'price'})).ok,true);assert.match(price.style.outline,/2px solid/);f.instance.destroy();assert.equal(price.style.outline,'1px dashed red');assert.equal(price.style.outlineOffset,'2px');
});
test('highlight cleanup preserves a later theme styling update',async t=>{
  const f=fixture(t),price=f.doc.querySelector('#bjPrice');await run(f,{type:'highlight',handle:'sample-bunny',section:'price'});price.style.outline='3px dotted blue';f.instance.destroy();assert.equal(price.style.outline,'3px dotted blue');
});
test('shipping reveals the actual native details disclosure',async t=>{
  const f=fixture(t),details=Array.from(f.doc.querySelectorAll('details')).find(d=>d.textContent.includes('Shipping & Returns'));
  assert.equal(details.open,false);assert.equal((await run(f,{type:'highlight',section:'shipping'})).ok,true);assert.equal(details.open,true);
});
test('never pretends unavailable story or zoom controls exist',async t=>{
  const f=fixture(t);assert.equal((await run(f,{type:'highlight',handle:'sample-bunny',section:'story'})).ok,false);assert.equal((await run(f,{type:'zoom',handle:'sample-bunny'},'zoom')).ok,false);assert.equal(f.nav.length,0);
});
test('current typed search uses inert encoded shop URL parameters',async t=>{
  const f=fixture(t,{root:'/fr/'}),res=await run(f,{type:'search',query:'moon & stars',sort:'price-asc',filter:'all'});assert.equal(res.ok,true);const u=new URL(f.nav[0]);assert.equal(u.origin,'https://britesjewelry.com');assert.equal(u.pathname,'/fr/search');assert.equal(u.searchParams.get('q'),'moon & stars');assert.equal(u.searchParams.get('type'),'product');assert.equal(u.searchParams.get('sort_by'),'price-ascending');assert.equal(f.addClicks(),0);
});
test('filter uses only exact categories actually linked by the current theme',async t=>{
  const f=fixture(t);assert.equal((await run(f,{type:'filter',filter:'earrings'})).ok,true);assert.equal(f.nav[0],'https://britesjewelry.com/collections/earrings');assert.equal((await run(f,{type:'filter',filter:'rings'},'rings')).ok,false);assert.equal(f.nav.length,1);
});
test('native collection sort exposes only the literal offered values',async t=>{
  const f=fixture(t,{url:'https://britesjewelry.com/collections/all',raw:null,html:'<main id="MainContent"><select id="bjcSort"><option value="best-selling">Featured</option><option value="price-ascending">Low</option><option value="price-descending">High</option></select></main>'});
  assert.equal((await run(f,{type:'sort',sort:'price-desc'})).ok,true);assert.equal(f.doc.querySelector('#bjcSort').value,'price-descending');assert.equal((await run(f,{type:'sort',sort:'title-asc'},'alpha')).ok,false);
});
test('checkout opens the ordinary bag for shopper review and never payment or an order',async t=>{
  const f=fixture(t);assert.equal((await run(f,{type:'checkout'})).ok,true);assert.equal(f.nav[0],'https://britesjewelry.com/cart');assert.equal((await run(f,{type:'checkout-complete'},'complete')).ok,false);assert.equal(f.nav.length,1);assert.equal(f.calls.length,0);
});
test('request metadata is required even for ordinary navigation',async t=>{const f=fixture(t);assert.equal((await f.instance.execute({type:'open',handle:'sample-bunny'},{})).ok,false);assert.equal(f.nav.length,0);});
test('destroy removes listeners and rejects further controls',async t=>{const f=fixture(t);f.instance.destroy();const res=await run(f,{type:'bag'});assert.equal(res.cancelled,true);assert.equal(f.nav.length,0);});
test('refresh sanitizes cart data and uses a market currency without storing its private payload',async t=>{
  const f=fixture(t);f.state.cart.currency='CAD';assert.equal(await f.instance.refresh(),true);assert.equal(f.instance.snapshot().bagControls.itemCount,2);assert.equal(JSON.stringify(f.instance.snapshot()).includes('PRIVATE'),false);
  const res=await run(f,{type:'options'});assert.equal(res.ok,true);assert.equal(res.products[0].currency,'CAD');
});
test('production origin and explicit locale root must be valid',t=>{
  for(const config of [{root:'//evil.example/'},{root:'/../'},{theme:'unknown'},{url:'https://competitor.example/products/sample-bunny'}]){const f=fixture(t,config);assert.equal(f.instance,null);}
});
test('standalone install respects explicit config and preserves an existing adapter',t=>{
  const f=fixture(t),prior={snapshot(){}};f.win.BritesStorefrontAdapter=prior;assert.equal(adapter.install(f.win),null);assert.equal(f.win.BritesStorefrontAdapter,prior);
});
test('unpublished Liquid integration is opt-in and escapes script-breaking product data',()=>{
  const liquid=fs.readFileSync(path.join(__dirname,'../../shopify/snippets/brites-concierge.liquid'),'utf8');
  assert.match(liquid,/{% if enable_storefront_adapter %}/);assert.match(liquid,/routes\.root_url/);assert.match(liquid,/cart\.currency\.iso_code/);assert.match(liquid,/product\.variants_count/);assert.match(liquid,/product \| json \| replace: '<', '\\u003c'/);assert.match(liquid,/brites-shopify-storefront-adapter\.js/);assert.equal(/cart \| json|customer \| json|cart\.note/.test(liquid),false);
  assert(liquid.indexOf("'brites-storefront-bridge.js'")<liquid.indexOf("'brites-shopify-storefront-adapter.js'"));assert(liquid.indexOf("'brites-shopify-storefront-adapter.js'")<liquid.indexOf("'brites-concierge.js'"));
});
for(const action of [
  {type:'bag',selector:'#bjAdd'},{type:'open',handle:'../checkout'},{type:'open',handle:'javascript:alert(1)'},{type:'open',handle:'sample-bunny',url:'https://evil.example'},
  {type:'search',query:'<script>alert(1)</script>'},{type:'search',query:'https://evil.example'},{type:'select-option',handle:'sample-bunny',variantId:'1001'},
  {type:'select-option',handle:'sample-bunny',variantId:'gid://shopify/ProductVariant/1001',optionName:'Metal Choice',optionValue:'Sterling Silver'},
  {type:'gift-preferences',giftNote:'PRIVATE',selector:'#gift-note'},{type:'checkout-complete'},{type:'bag-remove',lineId:'../checkout'},
])test(`rejects untrusted or unsupported ${JSON.stringify(action)}`,()=>assert.equal(adapter.validateAction(action),null));
test('does not invoke action getters before rejecting them',()=>{let called=false;const action={};Object.defineProperty(action,'type',{enumerable:true,get(){called=true;return 'bag';}});assert.equal(adapter.validateAction(action),null);assert.equal(called,false);});
test('inherited action getters cannot supply a requested handle or trigger execution',()=>{let called=false;const inherited={};Object.defineProperty(inherited,'handle',{get(){called=true;return 'sample-bunny';}});const action=Object.assign(Object.create(inherited),{type:'open'});assert.equal(adapter.validateAction(action),null);assert.equal(called,false);});
test('symbol and non-enumerable arbitrary action fields are rejected',()=>{const symbol={type:'bag',[Symbol('selector')]:'#bjAdd'};assert.equal(adapter.validateAction(symbol),null);const hidden={type:'bag'};Object.defineProperty(hidden,'selector',{value:'#bjAdd'});assert.equal(adapter.validateAction(hidden),null);});
test('action strings are never taken from an executable coercion hook',()=>{let called=false;const spoof={toString(){called=true;return 'bag';}};for(const action of [{type:spoof},{type:'sort',sort:spoof},{type:'filter',filter:spoof},{type:'select-option',handle:'sample-bunny',variantId:spoof}])assert.equal(adapter.validateAction(action),null);assert.equal(called,false);});
test('product projection rejects inconsistent options, duplicate IDs and unsafe integer IDs',()=>{
  const raw=product();assert.equal(adapter.projectProduct({...raw,id:Number.MAX_SAFE_INTEGER+1},'USD'),null);
  assert.equal(adapter.projectProduct({...raw,variants:[raw.variants[0],raw.variants[0]]},'USD'),null);
  assert.equal(adapter.projectProduct({...raw,options:[{name:'Metal Choice'},{name:'Metal Choice'}]},'USD'),null);
  assert.equal(adapter.projectProduct({...raw,variants:[{...raw.variants[0],price:12.1}]},'USD'),null);
});
test('250-variant responses without a trusted complete count remain incomplete',()=>{
  const raw={id:1,handle:'sample',title:'Sample',options:['Size'],variants:Array.from({length:250},(_,i)=>({id:i+1,title:String(i),price:100,available:true,options:[String(i)]}))};
  assert.equal(adapter.projectProduct(raw,'USD').variantsComplete,false);assert.equal(adapter.projectProduct(raw,'USD',251).variantsComplete,false);assert.equal(adapter.projectProduct(raw,'USD',250).variantsComplete,true);
});

function mappedProductFixture(t,groups,omitLast=false){
  let combinations=[[]];for(const group of groups)combinations=combinations.flatMap(prior=>group.values.map(value=>[...prior,value]));
  const raw={id:9001,handle:'sample-bunny',title:'Synthetic Template Piece',type:'Jewelry',options:groups.map(g=>({name:g.name})),variants:combinations.map((values,i)=>({id:1001+i,title:values.join(' / '),price:5000+i*100,available:true,options:values}))};
  if(omitLast)raw.variants.pop();
  const controls=groups.map((g,i)=>g.name==='Metal Choice'?`<div id="bjMetals" data-idx="${i+1}">${g.values.map((v,j)=>`<button type="button" data-vi="${j}" class="${j===0?'on':''}">${v}</button>`).join('')}</div>`:g.name==='Engraving'?`<div id="bjEngr" data-idx="${i+1}"><input id="bjEngrChk" type="checkbox"><input id="bjEngrTxt" value="PRIVATE-GIFT-NOTE"></div>`:`<div class="bjselx"><select class="bjOptSel" data-idx="${i+1}">${g.values.map(v=>`<option>${v}</option>`).join('')}</select><button type="button" class="bjselx__btn" aria-haspopup="listbox" aria-expanded="false">${g.values[0]}</button></div>`).join('');
  return fixture(t,{raw,html:`<main id="MainContent"><div id="shopify-section-product-template"><h1>Synthetic Template Piece</h1><span id="bjPrice">$50</span><form id="bjForm">${controls}<button type="button" data-q="-1">–</button><input id="bjQty" value="1" readonly><button type="button" data-q="1">+</button><button type="button" id="bjAdd">Add to Cart</button></form></div></main>`});
}
const metalGroup={name:'Metal Choice',values:['Sterling Silver','14k Gold Filled','14k Rose Gold Filled','14k Solid Gold']};
for(const [name,groups,requestedName,requestedValue] of [
  ['Bunny', [metalGroup,{name:'Necklace Length',values:['14 inch','16 inch','18 inch','20 inch']},{name:'Engraving',values:['None','Engraved']}], 'Engraving','Engraved'],
  ['Beady', [{name:'Metal Choice',values:['Sterling Silver','14k Gold Filled','14k Solid Gold']},{name:'Engraving',values:['No','Yes']},{name:'Chain Length',values:['14"','16"','18"']}], 'Engraving','Yes'],
  ['Stud', [metalGroup], 'Metal Choice','14k Gold Filled'],
  ['Hoop', [{name:'Metal Choice',values:['Sterling Silver','14k Gold Filled','14k Solid Gold']},{name:'Hoop Size',values:['8.5mm','11mm']}], 'Hoop Size','11mm'],
  ['Charm-only', [metalGroup,{name:'Charm Type',values:['Necklace CHARM','Charm + Engraving','Huggie CHARM SET']}], 'Charm Type','Huggie CHARM SET']
])test(`synthetic ${name} template preserves the exact groups mapped from live form inspection`,async t=>{
  const f=mappedProductFixture(t,groups),before=f.instance.snapshot().productControls;assert(before);assert(f.instance.capabilities.actions.includes('select-option'));
  const res=await run(f,{type:'select-option',handle:'sample-bunny',optionName:requestedName,optionValue:requestedValue});assert.equal(res.ok,true);
  for(const choice of res.snapshot.productControls.selectedOptions)assert.equal(choice.value,choice.name===requestedName?requestedValue:before.selectedOptions.find(old=>old.name===choice.name).value);
  assert.equal(f.addClicks(),0);assert.equal(JSON.stringify(res.snapshot).includes('PRIVATE'),false);
});
test('unobserved engraving word pairs stay unsupported instead of being semantically guessed',t=>{
  const f=mappedProductFixture(t,[metalGroup,{name:'Engraving',values:['Personalize','Plain']}]);assert.equal(f.instance.snapshot().productControls,null);assert(!f.instance.capabilities.actions.includes('select-option'));
});
test('exact option groups cannot invent an absent size and metal combination',async t=>{
  const groups=[{name:'Metal Choice',values:['Sterling Silver','14k Gold Filled','14k Solid Gold']},{name:'Hoop Size',values:['8.5mm','11mm']}],f=mappedProductFixture(t,groups,true);
  assert.equal((await run(f,{type:'select-option',handle:'sample-bunny',optionName:'Metal Choice',optionValue:'14k Solid Gold'},'metal')).ok,true);
  const res=await run(f,{type:'select-option',handle:'sample-bunny',optionName:'Hoop Size',optionValue:'11mm'},'size');assert.equal(res.ok,false);assert.equal(f.addClicks(),0);
});
