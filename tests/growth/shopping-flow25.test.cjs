'use strict';
// Actual widget behavior, synthetic storefront data; no microphone, GPU or live order claim.
const test=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const {JSDOM,VirtualConsole}=require('jsdom');
const source=fs.readFileSync(require.resolve('../../brites-concierge.js'),'utf8');
const clone=v=>JSON.parse(JSON.stringify(v));
const turn=()=>new Promise(r=>setImmediate(r));
async function settle(){await turn();await turn();await turn();}
function deferred(){let resolve;return {promise:new Promise(r=>{resolve=r;}),resolve:v=>resolve(v)};}
const variant={id:'gid://shopify/ProductVariant/25101',numericId:'25101',title:'Sterling Silver / 16 inch / None',price:54,available:true,options:[{name:'Metal',value:'Sterling Silver'},{name:'Length',value:'16 inch'},{name:'Engraving',value:'None'}]};
const piece={id:'gid://shopify/Product/251',handle:'compass-necklace',url:'https://britesjewelry.com/products/compass-necklace',title:'Compass Necklace',type:'Necklace',currency:'USD',variants:[variant],variantsComplete:true,minPrice:54,suggestedVariantId:variant.id,why:'A personal milestone reminder.'};
const second={...clone(piece),id:'gid://shopify/Product/252',handle:'bunny-necklace',url:'https://britesjewelry.com/products/bunny-necklace',title:'Bunny Necklace',variants:[{...clone(variant),id:'gid://shopify/ProductVariant/25201',numericId:'25201'}]};
function fixture(t,options={}){
  const errors=[],vc=new VirtualConsole();vc.on('jsdomError',e=>errors.push(e));
  const dom=new JSDOM('<!doctype html><body></body>',{url:'https://growth-sandbox.example/concierge-sandbox.html'+(options.query||''),runScripts:'outside-only',pretendToBeVisual:true,virtualConsole:vc});
  const w=dom.window,d=w.document,script=d.createElement('script');script.src='https://growth-sandbox.example/brites-concierge.js';script.dataset.sandbox='true';Object.defineProperty(d,'currentScript',{get:()=>script});
  const calls=[],poses=[];w.HTMLElement.prototype.scrollIntoView=function(){};
  w.BritesConciergeAvatar={create(){return {setState(){},setVisible(){},setEmotion(){},setPaused(){},setLevel(){},triggerGreeting(){},cue(){},focusProduct(v){poses.push(v);},clearFocus(){}};}};
  w.fetch=async(raw,init={})=>{
    const url=String(raw);calls.push({url,signal:init.signal,body:init.body});
    if(url.includes('/api/growth/product?')){const handle=new URL(url).searchParams.get('handle');const payload=options.read?await options.read(handle):{live:true,product:clone(handle===second.handle?second:piece)};return {ok:payload.ok!==false,json:async()=>payload};}
    const body=init.body?JSON.parse(init.body):{};
    return {ok:true,json:async()=>body.message?{live:true,reply:'These current pieces match your preferences.',preferences:{},products:[clone(piece)],meanings:[]}: {}};
  };
  w.eval(source);const root=d.querySelector('brites-concierge').shadowRoot;
  const button=text=>[...root.querySelectorAll('button')].find(n=>n.textContent.trim()===text||n.getAttribute('aria-label')===text);
  t.after(()=>{try{w.BritesConcierge.close();}catch{}w.close();});
  const cards=()=>[...root.querySelectorAll('.card')];
  return {w,d,root,button,calls,poses,errors,cards,open:async()=>{w.BritesConcierge.open();await settle();},page(handle){w.history.pushState({},'','/concierge-sandbox.html?product='+encodeURIComponent(handle));d.dispatchEvent(new w.CustomEvent('brites-concierge:page'));}};
}
function visible(node){for(let p=node;p;p=p.parentElement){if(p.hidden)return false;}return true;}

test('confirmed bag addition exposes View bag in the visible product flow while history stays optional',async t=>{
  const h=fixture(t);await h.open();h.button('Type instead').click();const input=h.root.querySelector('input');input.value='A compass necklace';h.root.querySelector('form').dispatchEvent(new h.w.Event('submit',{bubbles:true,cancelable:true}));await settle();h.button('Choose options').click();h.button('Review adding to bag').click();assert.equal(h.w.sessionStorage.getItem('brites-sandbox-cart'),null);h.button('Confirm add to bag').click();await settle();const go=h.button('View bag');assert.ok(go,'A confirmed add must expose the next step');assert.equal(h.root.querySelector('.messages').hidden,true);assert.equal(visible(go),true,'The bag control cannot be trapped in optional history');assert.equal(JSON.parse(h.w.sessionStorage.getItem('brites-sandbox-cart'))[0].variantId,variant.numericId);
});

test('opening concierge on a product page seeds the exact checked piece without asking or changing the cart',async t=>{
  const h=fixture(t,{query:'?product='+piece.handle});await h.open();assert.equal(h.calls.filter(c=>c.url.includes('/api/growth/product?')).length,1);assert.equal(h.cards()[0]?.dataset.productId,piece.id);assert.equal(h.w.sessionStorage.getItem('brites-sandbox-cart'),null);assert.equal(h.calls.some(c=>c.url.endsWith('/api/concierge')&&JSON.parse(c.body||'{}').message),false);assert.match(h.cards()[0].textContent,/Compass Necklace/);
});

for(const [name,product] of [
  ['wrong handle',{...clone(piece),handle:second.handle}],
  ['foreign product URL',{...clone(piece),url:'https://other.example/products/compass-necklace'}],
  ['wrong owned URL',{...clone(piece),url:second.url}],
  ['malformed variant identity',{...clone(piece),variants:[{...clone(variant),numericId:'999'}]}]
])test('current page seeding rejects '+name+' without displaying a substituted piece',async t=>{
  const h=fixture(t,{query:'?product='+piece.handle,read:async()=>({live:true,product})});await h.open();assert.equal(h.calls.filter(c=>c.url.includes('/api/growth/product?')).length,1);assert.equal(h.cards().length,0);assert.equal(h.w.sessionStorage.getItem('brites-sandbox-cart'),null);
});

test('closing during a current-page read prevents a late answer from installing a piece',async t=>{
  const pending=deferred(),h=fixture(t,{query:'?product='+piece.handle,read:()=>pending.promise});await h.open();assert.equal(h.calls.filter(c=>c.url.includes('/api/growth/product?')).length,1);assert.equal(h.cards().length,0);h.w.BritesConcierge.close();pending.resolve({live:true,product:clone(piece)});await settle();assert.equal(h.cards().length,0);assert.equal(h.w.sessionStorage.getItem('brites-sandbox-cart'),null);
});

test('current-page navigation race follows the latest page and ignores the old delayed product',async t=>{
  const pending=deferred(),h=fixture(t,{query:'?product='+piece.handle,read:handle=>handle===piece.handle?pending.promise:Promise.resolve({live:true,product:clone(second)})});await h.open();h.page(second.handle);await settle();pending.resolve({live:true,product:clone(piece)});await settle();assert.equal(h.cards()[0]?.dataset.productId,second.id);assert.equal(h.cards().some(card=>card.dataset.productId===piece.id),false);assert.equal(h.w.sessionStorage.getItem('brites-sandbox-cart'),null);
});
