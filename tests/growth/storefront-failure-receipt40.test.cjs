'use strict';
// Actual sandbox + widget. Public synthetic listing, no provider session, cart
// writes or microphone. Timers are shortened only for the existing 30s deadline.
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs');
const {JSDOM,VirtualConsole}=require('jsdom');
const source=Object.fromEntries(['concierge-sandbox.html','concierge-sandbox.js','brites-concierge.js','brites-storefront-bridge.js'].map(file=>[file,fs.readFileSync(require.resolve('../../'+file),'utf8')]));
const clone=v=>JSON.parse(JSON.stringify(v)),settle=async()=>{await new Promise(setImmediate);await new Promise(setImmediate);};
const piece={id:'gid://shopify/Product/4001',handle:'compass-necklace',title:'Compass Necklace',type:'Necklace',url:'https://britesjewelry.com/products/compass-necklace',description:'Published compass necklace.',currency:'USD',minPrice:55,variantsComplete:true,checkedAt:Date.now(),detailState:'checked',image:'https://cdn.shopify.com/compass-necklace.jpg',options:[{name:'Metal Choice',values:['Sterling Silver']}],variants:[{id:'gid://shopify/ProductVariant/40011',numericId:'40011',title:'Sterling Silver',price:55,available:true,options:[{name:'Metal Choice',value:'Sterling Silver'}]}]};
async function fixture(t,{mode='reject',shortDeadline=false}={}){
  const errors=[],vc=new VirtualConsole();vc.on('jsdomError',e=>errors.push(e));const dom=new JSDOM(source['concierge-sandbox.html'],{url:'https://preview.example/concierge-sandbox.html',runScripts:'outside-only',pretendToBeVisual:true,virtualConsole:vc}),w=dom.window,d=w.document,requests=[];
  d.body.dataset.inventoryPreload='off';w.matchMedia=()=>({matches:false,addEventListener(){}});w.HTMLElement.prototype.scrollIntoView=function(){};w.scrollTo=function(){};
  w.fetch=async(raw,init={})=>{const u=new URL(raw,w.location.href);requests.push({url:u,init});let body;
    if(u.pathname==='/api/growth/catalogue')body={live:true,products:[piece],pageInfo:{hasNextPage:false,endCursor:null}};
    else if(u.pathname==='/api/growth/product')body={live:true,product:piece,checkedAt:Date.now()};
    else if(u.pathname==='/api/growth/storefront-services')body={schema:1,guidance:{},offers:{items:[]},conflicts:[]};
    else if(u.pathname==='/api/growth/events'||u.pathname==='/api/concierge-voice')body={ok:true,enabled:false,nativeAudio:false};
    else if(u.pathname==='/api/concierge'){
      if(init.body&&JSON.parse(init.body).event)return {ok:true,json:async()=>({ok:true})};
      if(mode==='reject')throw Error('Synthetic provider offline');
      if(mode==='http')return {ok:false,json:async()=>({error:'Synthetic service unavailable'})};
      if(mode==='hang')return new Promise(()=>{});
      if(mode==='hang-body')return {ok:true,json:()=>new Promise(()=>{})};
      body={reply:null,preferences:{},products:[]};
    }else throw Error('Unexpected '+u.pathname);
    return {ok:true,json:async()=>clone(body)};
  };
  w.eval(source['brites-storefront-bridge.js']);w.eval(source['concierge-sandbox.js']);await settle();await w.BritesSandboxStorefront.execute({type:'open',handle:piece.handle});
  w.BritesConciergeAvatar={create(){return {setState(){},setEmotion(){},setVisible(){},setPaused(){},retry(){},triggerGreeting(){},clearFocus(){},focusProduct(){},setLevel(){},setSpeechSignal(){},clearProduct(){},showProduct(){},cancelPerformance(){},setFloating(){},cue(){},destroy(){}};}};
  const script=d.createElement('script');script.src='/brites-concierge.js';script.dataset.sandbox='true';Object.defineProperty(d,'currentScript',{get:()=>script});w.eval(source['brites-concierge.js']);w.BritesConcierge.open({focus:false});await settle();
  if(shortDeadline){const timer=w.setTimeout.bind(w);w.setTimeout=(callback,delay,...args)=>timer(callback,delay===30000?5:delay,...args);}
  const root=d.querySelector('brites-concierge').shadowRoot;t.after(()=>{w.BritesConcierge.close();w.close();});return {w,d,root,requests,errors,api:w.BritesConcierge,state:()=>JSON.parse(w.sessionStorage.getItem('brites-concierge-v1')),providers:()=>requests.filter(r=>r.url.pathname==='/api/concierge'&&r.init.body&&typeof JSON.parse(r.init.body).message==='string'),cart:()=>JSON.parse(w.sessionStorage.getItem('brites-sandbox-cart')||'[]')};
}
async function assertReleased(f){assert.equal(f.root.querySelector('.composer input').disabled,false);assert.equal(f.root.querySelector('.composer button[type=submit]').disabled,false);assert.equal(f.d.querySelector('#guide-request').disabled,false);assert.equal(f.d.querySelector('#guide-request-send').disabled,false);assert.equal(f.state().pendingTurn,null);const next=await f.api.sendShopperCommand('Open my bag');assert.equal(next.ok,true,next.error||next.reply);assert.equal(f.w.BritesSandboxStorefront.snapshot().pageKind,'bag');assert.equal(f.providers().length,1,'No automatic retry or provider route for the next exact control');assert.deepEqual(f.cart(),[]);assert.equal(f.errors.length,0);}
for(const [mode,expected] of [['reject',/Synthetic provider offline/],['http',/Synthetic service unavailable/],['malformed',/selection response could not be checked/]])test('typed '+mode+' failure returns an error receipt and releases composer controls',async t=>{const f=await fixture(t,{mode}),result=await f.api.sendShopperCommand('Tell me a story about this piece');assert.equal(result.ok,false);assert.match(result.error,expected);assert.match(f.root.textContent,expected);assert.equal(result.cartChanged,undefined);await assertReleased(f);});
test('the actual guide console displays the provider failure without claiming completion or staying disabled',async t=>{const f=await fixture(t),form=f.d.querySelector('#guide-request-form');f.d.querySelector('#guide-request').value='Tell me a story about this piece';form.dispatchEvent(new f.w.Event('submit',{bubbles:true,cancelable:true}));await settle();assert.equal(form.getAttribute('aria-busy'),'false');assert.match(f.d.querySelector('#guide-request-status').textContent,/Synthetic provider offline/);assert.doesNotMatch(f.d.querySelector('#guide-request-status').textContent,/request is complete/i);await assertReleased(f);});
for(const mode of ['hang','hang-body'])test('a stalled '+mode+' obeys the existing deadline and releases the actual guide/composer',async t=>{const f=await fixture(t,{mode,shortDeadline:true}),result=await f.api.sendShopperCommand('Tell me a story about this piece');assert.equal(result.ok,false);assert.match(result.error,/took too long/);assert.match(f.root.textContent,/took too long/);await assertReleased(f);});
test('closing a pending fallback cancels its receipt and unlocks the next exact control',async t=>{const f=await fixture(t,{mode:'hang'}),pending=f.api.sendShopperCommand('Tell me a story about this piece');await settle();assert.equal(f.root.querySelector('.composer input').disabled,true);f.api.close();const result=await pending;assert.equal(result.ok,false);assert.match(result.error,/cancelled/);f.api.open({focus:false});await settle();await assertReleased(f);});
