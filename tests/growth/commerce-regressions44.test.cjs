'use strict';
// Real connected host/bridge/widget and standalone legacy widget regressions.
// Only transport and avatar hardware are synthetic; no live Shopify claim.
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs');
const {JSDOM,VirtualConsole}=require('jsdom');
const code=name=>fs.readFileSync(require.resolve('../../'+name),'utf8');
const clone=value=>JSON.parse(JSON.stringify(value));
const settle=async()=>{for(let n=0;n<6;n++)await new Promise(setImmediate);};
function deferred(){let resolve;return {promise:new Promise(done=>{resolve=done;}),resolve:value=>resolve(value)};}
function piece(id=44101,title='Butterfly Hoop Earrings'){
  const handle='commerce44-'+id;
  return {id:'gid://shopify/Product/'+id,handle,title,type:'Earrings',url:'https://britesjewelry.com/products/'+handle,currency:'USD',description:'Checked synthetic butterfly earrings with the published material and hoop choices.',tags:['motif:butterfly'],image:'https://cdn.shopify.com/'+handle+'.jpg',images:[{url:'https://cdn.shopify.com/'+handle+'.jpg'}],checkedAt:Date.now(),detailState:'checked',variantsComplete:true,minPrice:43,why:'Checked butterfly motif.',options:[{name:'Metal Choice',values:['Sterling Silver','14k Gold Filled']},{name:'Hoop Size',values:['9 mm','11 mm']}],variants:['Sterling Silver','14k Gold Filled'].flatMap((metal,m)=>['9 mm','11 mm'].map((hoop,n)=>({id:'gid://shopify/ProductVariant/'+(id*10+m*2+n+1),numericId:String(id*10+m*2+n+1),title:metal+' / '+hoop,price:43+m*15+n*3,available:true,options:[{name:'Metal Choice',value:metal},{name:'Hoop Size',value:hoop}]})))};
}
async function fixture(t,{connected=true,restoredProduct=null,hostSingleVariant=false}={}){
  const errors=[],vc=new VirtualConsole();vc.on('jsdomError',e=>errors.push(e));
  const dom=new JSDOM(connected?code('concierge-sandbox.html'):'<!doctype html><body></body>',{url:'https://preview.example/concierge-sandbox.html',runScripts:'outside-only',pretendToBeVisual:true,virtualConsole:vc}),w=dom.window,d=w.document,rows=[piece(),piece(44102,'Rabbit Hoop Earrings')],requests=[];
  if(hostSingleVariant){rows[0].variants=[rows[0].variants[0]];rows[0].options=rows[0].options.map(group=>({name:group.name,values:[rows[0].variants[0].options.find(option=>option.name===group.name).value]}));}
  let fresh=null;
  delete d.body.dataset.catalogueSeed;w.matchMedia=()=>({matches:false,addEventListener(){},removeEventListener(){}});w.HTMLElement.prototype.scrollIntoView=function(){};w.scrollTo=()=>{};
  w.fetch=async(raw,init={})=>{
    const url=new URL(raw,w.location.href);requests.push({url,init});let body;
    if(url.pathname==='/api/growth/catalogue')body={live:true,checkedAt:Date.now(),products:rows,pageInfo:{hasNextPage:false,endCursor:null}};
    else if(url.pathname==='/api/growth/inventory')body={live:true,checkedAt:Date.now(),products:rows,inventory:{schema:1,total:rows.length,offset:0,loaded:rows.length,detailsLoaded:rows.length,ready:true,partial:false,expiresAt:Date.now()+300000},pageInfo:{hasNextPage:false,nextOffset:null}};
    else if(url.pathname==='/api/growth/product'){const p=rows.find(p=>p.handle===url.searchParams.get('handle'));body=fresh?await fresh(p,init):{live:true,checkedAt:Date.now(),product:p};}
    else if(url.pathname==='/api/growth/storefront-services')body={schema:1,guidance:{},conflicts:[],offers:{items:[]}};
    else if(url.pathname==='/api/growth/knowledge')body={live:true,meanings:[]};
    else if(['/api/growth/events','/api/concierge-voice'].includes(url.pathname))body={ok:true,enabled:true,nativeAudio:true,publicDemo:true};
    else if(url.pathname==='/api/concierge')body={live:true,checkedAt:Date.now(),reply:'These checked earrings are available to compare.',products:rows,meanings:[],preferences:{}};
    else throw Error('Unexpected commerce44 request '+url.pathname);
    return {ok:true,json:async()=>clone(body)};
  };
  if(connected){for(const name of ['brites-catalogue-intents.js','brites-concierge-shopping-guide.js','brites-storefront-bridge.js','concierge-sandbox.js'])w.eval(code(name));await settle();await w.BritesSandboxStorefront.preloadInventory();}
  w.BritesConciergeAvatar={create(){return {setState(){},setEmotion(){},setVisible(){},setPaused(){},retry(){},triggerGreeting(){},clearFocus(){},focusProduct(){},setLevel(){},setSpeechSignal(){},clearProduct(){},showProduct(){},cancelPerformance(){},setFloating(){},cue(){},destroy(){}};}};
  if(restoredProduct)w.sessionStorage.setItem('brites-concierge-v1',JSON.stringify({updatedAt:Date.now(),products:[restoredProduct],selectedVariants:{[restoredProduct.id]:restoredProduct.variants.at(-1).id},history:[],preferences:{},meanings:[],policyLinks:[],productHandles:[],uncertainVariants:[]}));
  const script=d.createElement('script');script.src='/brites-concierge.js';script.dataset.sandbox='true';Object.defineProperty(d,'currentScript',{get:()=>script});w.eval(code('brites-concierge.js'));await settle();w.BritesConcierge.open({focus:false});await settle();
  t.after(()=>{w.BritesConcierge.close();w.close();});
  const root=d.querySelector('brites-concierge').shadowRoot;
  return {w,d,rows,requests,errors,root,store:w.BritesSandboxStorefront,command:text=>w.BritesConcierge.sendShopperCommand(text),lookup:fn=>{fresh=fn;},cart:()=>JSON.parse(w.sessionStorage.getItem('brites-sandbox-cart')||'[]'),button(label,parent=root){return [...parent.querySelectorAll('button')].find(b=>b.textContent===label);},async discover(){await this.command('Show me butterfly earrings');return [...root.querySelectorAll('.card')].find(c=>c.dataset.productId===rows[0].id);}};
}
function savedLines(p,v){return Array.from({length:50},(_,i)=>({productId:p.id,title:p.title,variantId:v.numericId,variant:v.title,price:v.price,currency:p.currency,quantity:i===0?20:1,variantOptions:clone(v.options).sort((a,b)=>a.name.localeCompare(b.name))}));}
async function hostChoice(h){const card=await h.discover();assert(card);h.button('Choose options',card).click();await settle();assert.equal(h.store.snapshot().currentHandle,h.rows[0].handle);const select=h.d.querySelector('#piece-variant');select.value=h.rows[0].variants[0].id;select.dispatchEvent(new h.w.Event('change',{bubbles:true}));await settle();return card;}

test('a connected recommendation chooser opens the actual host menu with no automatic variant or second chooser',async t=>{
  const h=await fixture(t),card=await h.discover();h.button('Choose options',card).click();await settle();const pc=h.store.snapshot().productControls;
  assert.equal(pc.handle,h.rows[0].handle);assert.equal(pc.optionsOpen,true);assert.equal(pc.variantId,null);assert.deepEqual(clone(pc.selectedOptions),[]);assert.equal(h.d.querySelector('.option-menu').hidden,false);assert.equal(h.root.querySelector('.options select'),null);assert.deepEqual(h.cart(),[]);assert.deepEqual(h.errors,[]);
});
test('a connected restored 250-variant card cannot substitute its stale last selection for the current one-option host',async t=>{
  const restored=piece(),oldOptions=Array.from({length:250},(_,n)=>'Archived size '+(n+1));restored.options=[{name:'Published size',values:oldOptions}];restored.variants=oldOptions.map((value,n)=>({id:'gid://shopify/ProductVariant/'+(447001+n),numericId:String(447001+n),title:value,price:43+n,available:true,options:[{name:'Published size',value}]}));restored.suggestedVariantId=restored.variants.at(-1).id;
  const h=await fixture(t,{restoredProduct:restored,hostSingleVariant:true}),card=[...h.root.querySelectorAll('.card')].find(card=>card.dataset.productId===restored.id);assert(card);h.button('Choose options',card).click();await settle();const pc=h.store.snapshot().productControls;
  assert.equal(pc.productId,restored.id);assert.equal(pc.handle,restored.handle);assert.equal(pc.variantId,null);assert.deepEqual(clone(pc.selectedOptions),[]);assert.deepEqual(clone(pc.optionGroups),h.rows[0].options);assert.deepEqual([...h.d.querySelector('#piece-variant').options].map(option=>option.value),['',h.rows[0].variants[0].id]);assert.equal(h.root.querySelector('.options select'),null);assert.equal(h.root.querySelector('.review'),null);assert.deepEqual(h.cart(),[]);assert.deepEqual(h.errors,[]);
});
test('opening the connected card chooser again preserves exact current native choices and quantity',async t=>{
  const h=await fixture(t);await hostChoice(h);await h.command('Set quantity to 2');const before=clone(h.store.snapshot().productControls),card=[...h.root.querySelectorAll('.card')].find(c=>c.dataset.productId===h.rows[0].id);h.button('Choose options',card).click();await settle();const after=h.store.snapshot().productControls;
  assert.equal(after.variantId,before.variantId);assert.equal(after.quantity,2);assert.deepEqual(clone(after.selectedOptions),before.selectedOptions);assert.deepEqual(h.cart(),[]);assert.equal(h.root.querySelector('.options select'),null);
});
test('a manual connected chooser supersedes an unfinished factual read and still opens the actual options',async t=>{
  const h=await fixture(t),card=await h.discover(),gate=deferred(),entered=deferred();let delay=true;
  h.w.BritesSandboxStorefront={...h.store,async readProduct(handle){const result=await h.store.readProduct(handle);if(delay){delay=false;entered.resolve();await gate.promise;}return result;}};
  const pending=h.command('What is the price of Butterfly Hoop Earrings?');await entered.promise;h.button('Choose options',card).click();await settle();gate.resolve();const old=await pending;await settle();
  assert.notEqual(old.ok,true);assert.equal(h.store.snapshot().currentHandle,h.rows[0].handle);assert.equal(h.store.snapshot().productControls.optionsOpen,true);assert.equal(h.store.snapshot().productControls.variantId,null);assert.deepEqual(h.cart(),[]);assert.deepEqual(h.errors,[]);
});
test('connected card review refuses a full bag instead of dropping the earliest exact line',async t=>{
  const h=await fixture(t);await hostChoice(h);const saved=savedLines(h.rows[0],h.rows[0].variants[0]);h.w.sessionStorage.setItem('brites-sandbox-cart',JSON.stringify(saved));await h.command('Review adding this piece to my cart');const confirm=h.d.querySelector('.product-review .primary');assert(confirm);confirm.click();confirm.click();await settle();
  assert.deepEqual(h.cart(),saved);assert.equal(h.cart().reduce((n,row)=>n+row.quantity,0),69);assert.match(h.d.querySelector('#storefront-status').textContent,/50 lines/);assert.equal(h.root.querySelector('.review'),null);
});
test('connected card review keeps fresh same-ID option drift out of the bag',async t=>{
  const h=await fixture(t);await hostChoice(h);await h.command('Review adding this piece to my cart');h.lookup(p=>{const changed=clone(p);changed.variants[0].title='14k Gold Filled / 9 mm';changed.variants[0].options[0].value='14k Gold Filled';return {live:true,checkedAt:Date.now(),product:changed};});h.d.querySelector('.product-review .primary').click();await settle();
  assert.deepEqual(h.cart(),[]);assert.match(h.d.querySelector('#storefront-status').textContent,/changed/);
});
test('connected chooser exact review and duplicate confirmation add one real host line with quantity two',async t=>{
  const h=await fixture(t);await hostChoice(h);await h.command('Set quantity to 2');await h.command('Review adding this piece to my cart');const confirm=h.d.querySelector('.product-review .primary');confirm.click();confirm.click();await settle();const cart=h.cart();
  assert.equal(cart.length,1);assert.equal(cart[0].quantity,2);assert.equal(cart[0].variantId,h.rows[0].variants[0].numericId);assert.deepEqual(cart[0].variantOptions,clone(h.rows[0].variants[0].options).sort((a,b)=>a.name.localeCompare(b.name)));assert.equal(h.store.snapshot().bagControls.itemCount,2);assert.deepEqual(h.errors,[]);
});
test('the standalone legacy confirmation also preserves every line of a full test bag',async t=>{
  const h=await fixture(t,{connected:false}),card=await h.discover();h.button('Choose options',card).click();const v=h.rows[0].variants.find(v=>v.id===card.querySelector('select').value),saved=savedLines(h.rows[0],v);h.w.sessionStorage.setItem('brites-sandbox-cart',JSON.stringify(saved));h.button('Review adding to bag',card).click();h.button('Confirm add to bag',card).click();await settle();
  assert.deepEqual(h.cart(),saved);assert.match(h.root.textContent,/50 lines/);assert.deepEqual(h.errors,[]);
});
for(const drift of ['options','title','partial','duplicate','held','unavailable'])test('standalone legacy confirmation rejects fresh exact '+drift+' drift',async t=>{
  const h=await fixture(t,{connected:false}),card=await h.discover();h.button('Choose options',card).click();h.button('Review adding to bag',card).click();h.lookup(p=>{const changed=clone(p);if(drift==='options')changed.variants[0].options[0].value='14k Gold Filled';if(drift==='title')changed.variants[0].title='14k Gold Filled / 9 mm';if(drift==='partial')changed.variantsComplete=false;if(drift==='duplicate')changed.variants.push(clone(changed.variants[0]));if(drift==='held')changed.cartHold=true;if(drift==='unavailable')changed.variants[0].available=false;return {live:true,product:changed};});h.button('Confirm add to bag',card).click();await settle();assert.deepEqual(h.cart(),[]);assert.equal(h.root.textContent.includes('Added to the sandbox bag.'),false);assert.deepEqual(h.errors,[]);
});
for(const boundary of ['close','fresh'])test('a delayed standalone confirmed review cannot add after '+boundary,async t=>{
  const h=await fixture(t,{connected:false}),card=await h.discover(),gate=deferred(),entered=deferred();h.button('Choose options',card).click();h.button('Review adding to bag',card).click();h.lookup(async p=>{entered.resolve();await gate.promise;return {live:true,product:p};});h.button('Confirm add to bag',card).click();await entered.promise;if(boundary==='close')h.w.BritesConcierge.close();else h.button('Start fresh').click();gate.resolve();await settle();assert.deepEqual(h.cart(),[]);assert.equal(h.root.textContent.includes('Added to the sandbox bag.'),false);assert.deepEqual(h.errors,[]);
});
