'use strict';
// Mount the actual page, bridge and guide. Only the story's 10s deadline is
// shortened; stalled fetch/body promises deliberately ignore abort signals.
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs');
const {JSDOM,VirtualConsole}=require('jsdom');
const source=Object.fromEntries(['concierge-sandbox.html','concierge-sandbox.js','brites-concierge.js','brites-storefront-bridge.js'].map(file=>[file,fs.readFileSync(require.resolve('../../'+file),'utf8')]));
const clone=value=>JSON.parse(JSON.stringify(value)),settle=async()=>{await new Promise(setImmediate);await new Promise(setImmediate);};
const piece={id:'gid://shopify/Product/4011',handle:'compass-necklace',title:'Compass Necklace',type:'Necklace',url:'https://britesjewelry.com/products/compass-necklace',description:'Published compass necklace.',currency:'USD',variantsComplete:true,checkedAt:Date.now(),detailState:'checked',image:'https://cdn.shopify.com/compass-necklace.jpg',options:[{name:'Metal Choice',values:['Sterling Silver']}],variants:[{id:'gid://shopify/ProductVariant/40111',numericId:'40111',title:'Sterling Silver',price:55,available:true,options:[{name:'Metal Choice',value:'Sterling Silver'}]}]};
const approved=()=>({products:[{productId:piece.id,kind:'interpretation',text:'A compass can represent a personal sense of direction.',context:'A contemporary interpretation.',sources:[{title:'Museum reference',url:'https://www.metmuseum.org/art/collection/search/466813',checkedAt:Date.now()}]}]});
const response=body=>({ok:true,json:async()=>clone(body)});
async function fixture(t,{shortDeadline=false}={}){
  const errors=[],console=new VirtualConsole();console.on('jsdomError',error=>errors.push(error));
  const dom=new JSDOM(source['concierge-sandbox.html'],{url:'https://preview.example/concierge-sandbox.html',runScripts:'outside-only',pretendToBeVisual:true,virtualConsole:console}),w=dom.window,d=w.document,requests=[];let knowledge=()=>new Promise(()=>{});
  d.body.dataset.inventoryPreload='off';w.matchMedia=()=>({matches:false,addEventListener(){}});w.HTMLElement.prototype.scrollIntoView=function(){};w.scrollTo=function(){};
  w.fetch=async(raw,init={})=>{const url=new URL(raw,w.location.href);requests.push({url,init});
    if(url.pathname==='/api/growth/knowledge')return knowledge(init);
    if(url.pathname==='/api/growth/catalogue')return response({live:true,products:[piece],pageInfo:{hasNextPage:false,endCursor:null}});
    if(url.pathname==='/api/growth/product')return response({live:true,product:piece,checkedAt:Date.now()});
    if(url.pathname==='/api/growth/storefront-services')return response({schema:1,guidance:{},offers:{items:[]},conflicts:[]});
    if(['/api/growth/events','/api/concierge-voice'].includes(url.pathname))return response({ok:true,enabled:false,nativeAudio:false});
    if(url.pathname==='/api/concierge')return response({live:true,reply:'Welcome to the synthetic boutique.',products:[],meanings:[],preferences:{},preserveSelection:true});
    throw Error('Unexpected story recovery route '+url.pathname);
  };
  w.eval(source['brites-storefront-bridge.js']);w.eval(source['concierge-sandbox.js']);await settle();await w.BritesSandboxStorefront.execute({type:'open',handle:piece.handle});
  w.BritesConciergeAvatar={create(){return {setState(){},setEmotion(){},setVisible(){},setPaused(){},retry(){},triggerGreeting(){},clearFocus(){},focusProduct(){},setLevel(){},setSpeechSignal(){},clearProduct(){},showProduct(){},cancelPerformance(){},setFloating(){},cue(){},destroy(){}};}};
  const script=d.createElement('script');script.src='/brites-concierge.js';script.dataset.sandbox='true';Object.defineProperty(d,'currentScript',{get:()=>script});w.eval(source['brites-concierge.js']);w.BritesConcierge.open({focus:false});await settle();
  const root=d.querySelector('brites-concierge').shadowRoot,deadlines=new Set(),timer=w.setTimeout.bind(w),clear=w.clearTimeout.bind(w);
  w.setTimeout=(callback,delay,...args)=>{let id;id=timer(()=>{deadlines.delete(id);callback(...args);},shortDeadline&&delay===10000?5:delay);if(delay===10000)deadlines.add(id);return id;};
  w.clearTimeout=id=>{deadlines.delete(id);clear(id);};
  t.after(()=>{w.BritesConcierge.close();w.close();});
  return {w,d,root,deadlines,errors,requests,knowledge:next=>{knowledge=next;},story:()=>d.querySelector('[data-store-section="story"] button'),reads:()=>requests.filter(request=>request.url.pathname==='/api/growth/knowledge')};
}
for(const mode of ['fetch','body'])test('a never-settling story '+mode+' releases the actual button after its deadline and retries only on click',async t=>{
  const f=await fixture(t,{shortDeadline:true});if(mode==='body')f.knowledge(()=>({ok:true,json:()=>new Promise(()=>{})}));
  const button=f.story();button.click();assert.equal(button.disabled,true);assert.equal(f.deadlines.size,1);assert.equal(f.root.querySelector('.composer input').disabled,false);assert.equal(f.d.querySelector('#guide-request').disabled,false);
  await new Promise(resolve=>setTimeout(resolve,20));await settle();assert.equal(button.disabled,false);assert.equal(button.hidden,false);assert.equal(f.deadlines.size,0);assert.equal(f.reads().length,1);assert.equal(f.reads()[0].init.signal.aborted,true);assert.match(f.d.querySelector('#storefront-status').textContent,/story check took too long.*try again/i);assert.equal(f.d.querySelector('.reviewed-story'),null);
  f.knowledge(()=>response(approved()));button.click();await settle();assert.equal(f.reads().length,2);assert.equal(button.hidden,true);assert.equal(button.disabled,false);assert.match(f.d.querySelector('.reviewed-story').textContent,/personal sense of direction/);assert.equal(f.d.querySelector('.reviewed-story a').href,'https://www.metmuseum.org/art/collection/search/466813');assert.equal(f.deadlines.size,0);
  const bag=await f.w.BritesConcierge.sendShopperCommand('Open my bag');assert.equal(bag.ok,true,bag.error||bag.reply);assert.equal(f.w.BritesSandboxStorefront.snapshot().pageKind,'bag');assert.equal(f.reads().length,2);assert.equal(f.root.querySelector('.composer input').disabled,false);assert.deepEqual(f.errors,[]);
});
test('navigation cancels a pending story and its late approved response cannot alter the new page or notice',async t=>{
  const f=await fixture(t);let release;f.knowledge(()=>new Promise(resolve=>{release=resolve;}));const button=f.story(),box=button.closest('.story-block');button.click();assert.equal(button.disabled,true);assert.equal(f.deadlines.size,1);
  const bag=await f.w.BritesConcierge.sendShopperCommand('Open my bag');assert.equal(bag.ok,true,bag.error||bag.reply);await settle();assert.equal(f.reads()[0].init.signal.aborted,true);assert.equal(f.deadlines.size,0);assert.equal(button.disabled,false);assert.equal(box.isConnected,false);const notice=f.d.querySelector('#storefront-status').textContent;
  release(response(approved()));await settle();assert.equal(f.w.BritesSandboxStorefront.snapshot().pageKind,'bag');assert.equal(box.querySelector('.reviewed-story'),null);assert.equal(f.d.querySelector('.reviewed-story'),null);assert.equal(f.d.querySelector('#storefront-status').textContent,notice);assert.equal(f.reads().length,1);assert.equal(f.root.querySelector('.composer input').disabled,false);assert.deepEqual(f.errors,[]);
});
