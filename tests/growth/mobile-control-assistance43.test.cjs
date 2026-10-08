'use strict';
// Actual widget + actual sandbox/native hosts. Synthetic rectangles establish
// reveal ordering and the exposed-region contract; browser QA establishes
// rendered dimensions, overflow and physical touch/focus visibility.
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs');
const {JSDOM,VirtualConsole}=require('jsdom');
const Adapter=require('../../brites-shopify-storefront-adapter.js');
const source=name=>fs.readFileSync(require.resolve('../../'+name),'utf8');
const clone=structuredClone,settle=async()=>{await new Promise(setImmediate);await new Promise(setImmediate);};
const rect=(left,top,width,height)=>({left,top,width,height,right:left+width,bottom:top+height});
function product(){return {id:'gid://shopify/Product/43601',handle:'mobile-hoop-earrings',title:'Mobile Hoop Earrings',type:'Earrings',description:'These published hoops are 10 mm wide.',url:'https://britesjewelry.com/products/mobile-hoop-earrings',currency:'USD',checkedAt:Date.now(),detailState:'checked',variantsComplete:true,image:'https://cdn.shopify.com/mobile-hoops.jpg',images:[{url:'https://cdn.shopify.com/mobile-hoops.jpg'}],options:[{name:'Metal Choice',values:['Sterling Silver','Gold Filled']},{name:'Hoop Size',values:['8mm','10mm']}],variants:['Sterling Silver','Gold Filled'].flatMap((metal,m)=>['8mm','10mm'].map((size,n)=>({id:'gid://shopify/ProductVariant/'+(436101+m*2+n),numericId:String(436101+m*2+n),title:metal+' / '+size,price:55+m*20+n*5,available:true,options:[{name:'Metal Choice',value:metal},{name:'Hoop Size',value:size}]})))};}
async function fixture(t,{native=false,width=360,height=720}={}){
  const errors=[],vc=new VirtualConsole();vc.on('jsdomError',error=>errors.push(error));
  const html=native?'<main id="MainContent"><section id="shopify-section-product-template"><h1 class="pi__title">Mobile Hoop Earrings</h1><p id="bjPrice">$55</p><div id="bjMedia"><figure class="m"><img src="https://cdn.shopify.com/mobile-hoops.jpg"></figure><div id="mDots"><i class="on"></i></div></div><form id="bjForm"><div id="bjMetals" data-idx="1"><button type="button" data-vi="0">Sterling Silver</button><button type="button" data-vi="1">Gold Filled</button></div><div class="bjselx"><select class="bjOptSel" data-idx="2"><option value="" disabled selected>Choose a size</option><option>8mm</option><option>10mm</option></select><button type="button" class="bjselx__btn" aria-haspopup="listbox" aria-expanded="false" aria-controls="mobile-native-size">Choose a size</button><div id="mobile-native-size" role="listbox" hidden><button type="button" role="option">8mm</button><button type="button" role="option">10mm</button></div></div><input id="bjQty" value="1" readonly><button type="button" data-q="-1">–</button><button type="button" data-q="1">+</button></form></section></main>':source('concierge-sandbox.html');
  const dom=new JSDOM(html,{url:native?'https://britesjewelry.com/products/mobile-hoop-earrings':'https://preview.example/concierge-sandbox.html',runScripts:'outside-only',pretendToBeVisual:true,virtualConsole:vc}),w=dom.window,d=w.document,p=product(),requests=[],scrolls=[],calls={avatarCreates:0,avatarDestroys:0};
  w.innerWidth=width;w.innerHeight=height;delete d.body.dataset.catalogueSeed;let offset=0,store;
  Object.defineProperty(w,'scrollY',{get:()=>offset});
  w.matchMedia=query=>({matches:/min-width:\s*1100px/.test(query)?w.innerWidth>=1100:false,addEventListener(){},removeEventListener(){}});
  function assisting(){return d.querySelector('brites-concierge')?.getAttribute('data-storefront-control-assist')==='true';}
  function documentBounds(node){
    if(node.matches?.('#bjMetals'))return rect(24,1700,300,144);
    if(node.matches?.('#bjMetals button'))return rect(30,1732+[...node.parentElement.children].indexOf(node)*48,288,44);
    if(node.matches?.('.bjselx'))return rect(24,1870,300,44);
    if(node.matches?.('.bjselx__btn,.bjOptSel'))return rect(24,1870,300,44);
    if(node.matches?.('[role="listbox"]'))return rect(24,1914,300,100);
    if(node.matches?.('[role="option"]'))return rect(28,1918+[...node.parentElement.children].indexOf(node)*48,292,44);
    if(node.matches?.('.option-group'))return node.hidden?rect(0,0,0,0):rect(24,1740,300,144);
    if(node.matches?.('.option-menu'))return node.hidden?rect(0,0,0,0):rect(24,1728,300,168);
    if(node.matches?.('.option-menu-trigger'))return rect(24,1680,300,46);
    if(node.matches?.('.option-area,#bjForm'))return rect(24,1550,300,640);
    if(node.matches?.('.product-price,#bjPrice'))return rect(24,1200,300,32);
    return rect(24,1000,300,44);
  }
  w.HTMLElement.prototype.getBoundingClientRect=function(){
    if(this.matches('.panel')&&this.getRootNode().host?.tagName==='BRITES-CONCIERGE'){const small=w.innerWidth<=640&&assisting(),panelHeight=small?Math.min(400,w.innerHeight*.54):w.innerHeight-82,top=small?w.innerHeight-8-panelHeight:17;return rect(8,top,w.innerWidth-16,panelHeight);}
    const r=documentBounds(this);return rect(r.left,r.top-offset,r.width,r.height);
  };
  w.scrollBy=args=>{scrolls.push({kind:'by',args:clone(args),assist:assisting(),snapshot:clone(store?.snapshot())});offset=Math.max(0,offset+args.top);};
  w.scrollTo=args=>{offset=typeof args==='object'?args.top||0:0;};
  w.HTMLElement.prototype.scrollIntoView=function(args){const r=documentBounds(this);scrolls.push({kind:'into',node:this,args:clone(args),assist:assisting(),snapshot:clone(store?.snapshot())});offset=Math.max(0,r.top-(args?.block==='center'?(w.innerHeight-r.height)/2:0));};
  const raw={id:43601,handle:p.handle,title:p.title,type:p.type,description:p.description,options:p.options.map(g=>g.name),images:[p.image],variants:p.variants.map(v=>({id:Number(v.numericId),title:v.title,price:Math.round(v.price*100),available:true,options:v.options.map(o=>o.value)}))};
  w.fetch=async(value,init={})=>{
    const url=new URL(String(value),w.location.href);requests.push({url,init});let body;
    if(url.pathname==='/cart.js')body={currency:'USD',item_count:0,items:[]};
    else if(url.pathname==='/products/mobile-hoop-earrings.js')body=raw;
    else if(url.pathname==='/api/growth/catalogue')body={live:true,checkedAt:Date.now(),products:[p],pageInfo:{hasNextPage:false,endCursor:null}};
    else if(url.pathname==='/api/growth/inventory')body={live:true,checkedAt:Date.now(),products:[p],inventory:{schema:1,total:1,offset:0,limit:24,loaded:1,detailsLoaded:1,ready:true,partial:false,expiresAt:Date.now()+300000},pageInfo:{hasNextPage:false,nextOffset:null}};
    else if(url.pathname==='/api/growth/product')body={live:true,checkedAt:Date.now(),product:p};
    else if(url.pathname==='/api/growth/storefront-services')body={schema:1,guidance:{},conflicts:[],offers:{items:[]}};
    else if(url.pathname==='/api/growth/knowledge')body={live:true,meanings:[]};
    else if(['/api/growth/events','/api/concierge-voice'].includes(url.pathname))body={ok:true,enabled:false,nativeAudio:false};
    else if(url.pathname==='/api/concierge'&&JSON.parse(init.body||'{}').event)body={ok:true};
    else throw Error('Unexpected mobile control request '+url.pathname);
    return {ok:true,json:async()=>clone(body)};
  };
  for(const name of ['brites-catalogue-intents.js','brites-concierge-shopping-guide.js','brites-storefront-bridge.js'])w.eval(source(name));
  if(native){
    d.querySelectorAll('#bjMetals button').forEach(button=>button.onclick=()=>d.querySelectorAll('#bjMetals button').forEach(other=>other.classList.toggle('on',other===button)));
    d.querySelector('.bjselx__btn').onclick=e=>{const open=e.currentTarget.getAttribute('aria-expanded')!=='true';e.currentTarget.setAttribute('aria-expanded',String(open));d.querySelector('[role="listbox"]').hidden=!open;};
    store=Adapter.create({window:w,root:'/',theme:'brites-v1',currency:'USD',product:raw,variantCount:4,fetch:w.fetch});w.BritesStorefrontAdapter=store;
  }else{w.eval(source('concierge-sandbox.js'));await settle();store=w.BritesSandboxStorefront;await store.preloadInventory();}
  w.BritesConciergeAvatar={create(){calls.avatarCreates++;return {setState(){},setEmotion(){},setVisible(){},setPaused(){},setLevel(){},setFloating(){},cancelPerformance(){},triggerGreeting(){},cue(){},focusProduct(){},clearFocus(){},showProduct(){},clearProduct(){},destroy(){calls.avatarDestroys++;}};}};
  const script=d.createElement('script');script.src=new URL('/brites-concierge.js',w.location.href).href;if(!native)script.dataset.sandbox='true';Object.defineProperty(d,'currentScript',{get:()=>script});w.eval(source('brites-concierge.js'));await settle();w.BritesConcierge.open({focus:false});await settle();
  t.after(()=>{w.BritesConcierge.close();store.destroy?.();w.close();});
  const host=d.querySelector('brites-concierge'),root=host.shadowRoot,command=text=>w.BritesConcierge.sendShopperCommand(text);
  if(!native){const result=await command('Open Mobile Hoop Earrings');assert.equal(result.ok,true,result.reply);await settle();}
  scrolls.length=0;
  return {w,d,p,host,root,store,command,scrolls,requests,calls,errors,resize(nextWidth,nextHeight){w.innerWidth=nextWidth;w.innerHeight=nextHeight;w.dispatchEvent(new w.Event('resize'));},bounds:node=>node.getBoundingClientRect()};
}
function exposed(f,node){const panel=f.bounds(f.root.querySelector('.panel')),r=f.bounds(node);assert.ok(r.top>=12,'actual menu header is in the exposed upper region');assert.ok(r.bottom<=panel.top-12,'actual literal choices are above the guide');}
for(const native of [false,true])test((native?'native':'sandbox')+' phone walkthrough publishes the actual menu before revealing its header and literal choices',async t=>{
  const f=await fixture(t,{native}),before=clone(f.store.snapshot().productControls),host=f.host,stage=f.root.querySelector('.concierge-avatar-stage'),input=f.root.querySelector('.composer input');
  const result=await f.command('Help me choose this piece');assert.equal(result.ok,true,result.reply);assert.equal(f.host.getAttribute('data-storefront-control-assist'),'true');
  assert.ok(f.scrolls.length);assert.equal(f.scrolls[0].assist,true,'compact state is published before the first reveal');assert.equal(f.scrolls[0].snapshot.productControls.optionsOpen,true,'menu state is real before scrolling');
  const menu=native?f.d.querySelector('#bjMetals'):f.d.querySelector('.option-group[data-option-name="Metal Choice"]');exposed(f,menu);
  const after=f.store.snapshot().productControls;assert.deepEqual(clone(after.selectedOptions),before.selectedOptions);assert.equal(after.quantity,before.quantity);assert.equal(f.d.querySelector('brites-concierge'),host);assert.equal(f.root.querySelector('.concierge-avatar-stage'),stage);assert.equal(f.calls.avatarCreates,1);assert.equal(f.calls.avatarDestroys,0);
  assert.equal(f.root.querySelector('.shopping-help').hidden,false,'guide chips remain available');assert.ok(f.root.querySelector('.shopping-help button'));input.focus();assert.equal(f.root.activeElement,input);assert.equal(input.disabled,false);
  assert.equal(f.requests.some(request=>request.url.pathname.endsWith('/cart/add.js')),false);assert.deepEqual(f.errors,[]);
});
test('a native partial choice reveals the actual next dropdown including options outside its wrapper flow rectangle',async t=>{
  const f=await fixture(t,{native:true});await f.command('Help me choose this piece');f.scrolls.length=0;
  const result=await f.command('Select Sterling Silver');assert.equal(result.ok,true,result.reply);
  assert.equal(f.d.querySelector('.bjselx__btn').getAttribute('aria-expanded'),'true');assert.equal(f.d.querySelector('[role="listbox"]').hidden,false);assert.equal(f.store.snapshot().productControls.openedOption,'Hoop Size');exposed(f,f.d.querySelector('[role="listbox"]'));exposed(f,f.d.querySelector('.bjselx__btn'));
  assert.equal(f.d.querySelector('.bjOptSel').value,'','revealing a dropdown does not choose the remaining axis');assert.equal(f.store.snapshot().productControls.variantId,null);assert.equal(f.store.snapshot().productControls.quantity,1);assert.deepEqual(f.errors,[]);
});
for(const mutation of ['quantity','choice','page'])test('a native dropdown handler changing '+mutation+' cannot be accepted as only a menu opening',async t=>{
  const f=await fixture(t,{native:true});f.d.querySelector('.bjselx__btn').addEventListener('click',()=>{
    if(mutation==='quantity')f.d.querySelector('#bjQty').value='3';
    else if(mutation==='choice')f.d.querySelector('#bjMetals button').classList.add('on');
    else f.w.history.replaceState({},'', '/products/another-product');
  });
  const result=await f.store.execute({type:'options',handle:f.p.handle,optionName:'Hoop Size'},{requestId:'mobile-hostile-'+mutation});
  assert.equal(result.ok,false);assert.equal(result.stale,true);assert.match(result.reason,/selection changed/);assert.equal(f.scrolls.length,0,'changed controls are not revealed as a successful request');assert.equal(f.requests.some(request=>request.url.pathname.endsWith('/cart/add.js')),false);await settle();assert.deepEqual(f.errors,[]);
});
test('a canonical context listener changing the native choice after publication cannot revive a stale reveal',async t=>{
  const f=await fixture(t,{native:true});let changed=false;
  f.d.addEventListener('brites-storefront:context',event=>{if(!changed&&event.detail.productControls?.optionsOpen){changed=true;f.d.querySelector('#bjQty').value='3';}});
  const result=await f.store.execute({type:'options',handle:f.p.handle,optionName:'Hoop Size'},{requestId:'mobile-hostile-context'});
  assert.equal(changed,true);assert.equal(result.ok,false);assert.equal(result.stale,true);assert.equal(f.scrolls.length,0);assert.equal(f.requests.some(request=>request.url.pathname.endsWith('/cart/add.js')),false);await settle();assert.deepEqual(f.errors,[]);
});
test('closing, dismissing, reopening and resizing retain the guide and retire only the compact menu layout',async t=>{
  const f=await fixture(t);await f.command('Help me choose this piece');const stage=f.root.querySelector('.concierge-avatar-stage'),input=f.root.querySelector('.composer input');input.focus();
  let result=await f.command('Close the options menu');assert.equal(result.ok,true,result.reply);assert.equal(f.host.getAttribute('data-storefront-control-assist'),'false');assert.equal(f.root.activeElement,input);
  await f.command('Help me choose this piece');f.w.BritesConcierge.close();assert.equal(f.host.getAttribute('data-storefront-control-assist'),'false');assert.equal(f.store.snapshot().productControls.optionsOpen,true);f.w.BritesConcierge.open({focus:false});assert.equal(f.host.getAttribute('data-storefront-control-assist'),'true');
  f.resize(900,760);f.scrolls.length=0;result=await f.command('Open the Metal Choice menu');assert.equal(result.ok,true,result.reply);assert.equal(f.scrolls.some(scroll=>scroll.kind==='by'),false,'wide view retains its ordinary reveal');assert.equal(f.scrolls.some(scroll=>scroll.kind==='into'),true);
  assert.equal(f.root.querySelector('.concierge-avatar-stage'),stage);assert.equal(f.calls.avatarCreates,1);assert.equal(f.calls.avatarDestroys,0);assert.deepEqual(f.errors,[]);
});
test('an actual open image and leaving the product retire option assistance without altering image mode',async t=>{
  const f=await fixture(t);await f.command('Help me choose this piece');let result=await f.command('Enlarge this piece');assert.equal(result.ok,true,result.reply);assert.equal(f.host.getAttribute('data-storefront-image-open'),'true');assert.equal(f.host.getAttribute('data-storefront-control-assist'),'false');
  result=await f.command('Close the image');assert.equal(result.ok,true,result.reply);assert.equal(f.host.getAttribute('data-storefront-image-open'),'false');assert.equal(f.host.getAttribute('data-storefront-control-assist'),'true');
  result=await f.command('Show all jewelry');assert.equal(result.ok,true,result.reply);assert.equal(f.store.snapshot().pageKind,'collection');assert.equal(f.host.getAttribute('data-storefront-control-assist'),'false');assert.deepEqual(f.errors,[]);
});
test('compact phone CSS reserves a bounded scrollable panel and keeps the real help, composer and close controls',t=>{
  const dom=new JSDOM('<!doctype html>');t.after(()=>dom.window.close());const style=dom.window.document.createElement('style');style.textContent=source('brites-concierge.css');dom.window.document.head.appendChild(style);
  const media=[...style.sheet.cssRules].find(rule=>rule.conditionText==='(max-width:640px)'&&[...rule.cssRules].some(child=>child.selectorText?.includes('data-storefront-control-assist')));assert.ok(media);
  const rules=[...media.cssRules],rule=suffix=>rules.find(rule=>rule.selectorText.endsWith(suffix)),panel=rule(' .panel');assert.equal(panel.style.getPropertyValue('display'),'grid');assert.match(panel.style.getPropertyValue('max-height'),/54dvh/);assert.equal(panel.style.getPropertyValue('overflow-y'),'auto');assert.equal(panel.style.getPropertyValue('overflow-x'),'hidden');assert.match(panel.style.getPropertyValue('grid-template-areas'),/"help help"/);assert.match(panel.style.getPropertyValue('grid-template-areas'),/"composer composer"/);
  assert.notEqual(rule(' .shopping-help').style.getPropertyValue('display'),'none');assert.equal(rule(' .shopping-help-choices .chip').style.getPropertyValue('min-height'),'44px');assert.equal(rule(' .composer input').style.getPropertyValue('font-size'),'16px');assert.equal(rule(' .composer input').style.getPropertyValue('min-height'),'44px');assert.equal(rule(' .composer').style.getPropertyValue('position'),'sticky');assert.equal(rule(' .icon').style.getPropertyValue('height'),'44px');
  assert.ok(rules.every(rule=>!rule.selectorText.includes('data-storefront-image-open')),'image mode keeps its existing distinct responsive rules');
});
