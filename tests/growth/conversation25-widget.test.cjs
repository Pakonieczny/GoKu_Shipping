'use strict';
// Synthetic DOM/network regressions only: no native audio, provider, GPU or
// buyer-engagement claims follow from these tests.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const {JSDOM, VirtualConsole} = require('jsdom');
const source = fs.readFileSync(require.resolve('../../brites-concierge.js'), 'utf8');
const tick = () => new Promise(resolve => setImmediate(resolve));
async function settle() { await tick(); await tick(); }
const clone = value => JSON.parse(JSON.stringify(value));
const variants = [{id:'gid://shopify/ProductVariant/101',numericId:'101',title:'Sterling Silver',price:54,available:true,options:[{name:'Metal',value:'Sterling Silver'}]}, {id:'gid://shopify/ProductVariant/102',numericId:'102',title:'Gold Filled',price:64,available:true,options:[{name:'Metal',value:'Gold Filled'}]}];
const piece = {id:'gid://shopify/Product/1',handle:'bunny-1',url:'https://britesjewelry.com/products/bunny-1',title:'Bunny Necklace',type:'Necklace',currency:'USD',variants,variantsComplete:true,suggestedVariantId:variants[0].id,minPrice:54,why:'A personal interpretation of a fresh start.'};
const preferences = {metal:'silver',budget:70,budgetCurrency:'USD',milestone:'graduation'};
const meaning = {productId:piece.id,text:'A fresh start can be your personal interpretation.',context:'Your graduation',sources:[{title:'Museum reference',url:'https://www.metmuseum.org/'}]};
const policy = {label:'Shipping policy',url:'https://britesjewelry.com/policies/shipping-policy'};
const shopping = {live:true,reply:'Here is a silver bunny.',preferences,products:[piece],meanings:[meaning],policyLinks:[policy]};
const greeting = {live:true,reply:'Hello! Lovely to meet you.',question:null,preferences,products:[],conversationOnly:true,conversationKind:'greeting'};
const modelNeeded = {...greeting,reply:'I can chat while you browse.',needsModelConversation:true,conversationKind:'general'};
const response = (body,ok=true) => ({ok,status:ok?200:503,json:async()=>clone(body)});
function deferred() { let resolve; const promise = new Promise(done=>{resolve=done;});return {promise,resolve}; }

function harness(t, options={}) {
  const errors=[],virtualConsole=new VirtualConsole();virtualConsole.on('jsdomError',error=>errors.push(error));
  const dom=new JSDOM('<!doctype html><html lang="en"><body><button id="shop">Shop</button></body></html>',{url:options.url||'https://growth-sandbox.example/concierge-sandbox.html',runScripts:'outside-only',pretendToBeVisual:true,virtualConsole});
  const win=dom.window,document=win.document,current=document.createElement('script');current.src='https://growth-sandbox.example/brites-concierge.js';current.dataset.sandbox='true';Object.defineProperty(document,'currentScript',{get:()=>current});
  let hidden=false;Object.defineProperty(document,'hidden',{get:()=>hidden});
  const network=[],avatar={focus:[],clear:0,cues:[],states:[]},contexts=[];
  win.BritesConciergeAvatar={create:()=>({setState:value=>avatar.states.push(value),setEmotion(){},setVisible(){},setPaused(){},setLevel(){},retry(){},destroy(){},triggerGreeting(){},focusProduct:value=>avatar.focus.push(clone(value)),clearFocus(){avatar.clear++;},cue:value=>avatar.cues.push(value)})};
  let voiceConfig;win.BritesConciergeVoice={create:config=>{voiceConfig=config;return {start:async()=>true,stop:async()=>{},dispose:async()=>{},updateContext:value=>contexts.push(clone(value))};}};
  win.HTMLElement.prototype.scrollIntoView=function(){};
  let messages=0;
  win.fetch=async(raw,init={})=>{const url=new URL(raw,win.location.href),body=init.body?JSON.parse(init.body):null;const call={url,body,signal:init.signal};network.push(call);
    if(url.pathname==='/api/concierge'&&body?.event)return response({});
    if(url.pathname==='/api/concierge'&&body?.message){messages++;return response(options.fast?options.fast(body,messages):messages===1?shopping:greeting);}
    if(url.pathname==='/api/concierge-demo-turn')return options.model?options.model(call):response({conversationOnly:true,reply:'Let’s talk about that.',aiUsed:true});
    throw Error('Unexpected write/provider route: '+url.pathname);
  };
  win.eval(source);const root=document.querySelector('brites-concierge').shadowRoot;
  const button=text=>Array.from(root.querySelectorAll('button')).find(value=>value.textContent.trim()===text||value.getAttribute('aria-label')===text);
  t.after(()=>{try{win.BritesConcierge.close();}catch{}win.close();});
  return {win,root,network,avatar,contexts,errors,button,get voiceConfig(){return voiceConfig;},
    open:()=>win.BritesConcierge.open(),close:()=>win.BritesConcierge.close(),
    snapshot:()=>JSON.parse(win.sessionStorage.getItem('brites-concierge-v1')),
    async ask(message){const input=root.querySelector('input[aria-label="Message the gift concierge"]');input.value=message;root.querySelector('form').dispatchEvent(new win.Event('submit',{bubbles:true,cancelable:true}));await settle();},
    async activateVoice(){button('Talk to me').click();await settle();root.querySelector('script[src$="brites-concierge-voice.js"]')?.dispatchEvent(new win.Event('load'));await settle();},
    hide(value){hidden=value;document.dispatchEvent(new win.Event('visibilitychange'));}
  };
}

test('hello preserves the exact selected card, chosen variant, meanings, policy and preferences',async t=>{
  const h=harness(t);h.open();await h.ask('Show me silver bunnies');const card=h.root.querySelector('.card');card.querySelector('.choose').click();const select=card.querySelector('select');select.value=variants[1].id;select.dispatchEvent(new h.win.Event('change'));
  await h.ask('Hello');assert.equal(h.root.querySelector('.card'),card);assert.equal(card.querySelector('select'),select);assert.equal(select.value,variants[1].id);
  const saved=h.snapshot();assert.deepEqual(saved.preferences,preferences);assert.deepEqual(saved.meanings,[meaning]);assert.deepEqual(saved.policyLinks,[policy]);assert.deepEqual(saved.productHandles,[piece.handle]);assert.equal(saved.selectedVariants[piece.id],variants[1].id);
  assert.equal(h.root.querySelector('.caption-text').textContent,greeting.reply);assert.equal(h.avatar.cues.at(-1),'greet');assert.equal(h.network.filter(call=>call.url.pathname==='/api/concierge-demo-turn').length,0);assert.equal(h.win.sessionStorage.getItem('brites-sandbox-cart'),null);
});

test('general conversation sends bounded public context and accepts only the model reply',async t=>{
  const malicious={conversationOnly:true,reply:'I enjoy the little stories people bring to jewellery.',aiUsed:true,products:[{...piece,id:'gid://shopify/Product/999',handle:'intruder',url:'https://britesjewelry.com/products/intruder',title:'Unverified intruder'}],preferences:{budget:999},meanings:[{productId:piece.id,text:'Invented history'}],requestedAction:{type:'navigate',productId:piece.id,url:piece.url},question:'Buy now?',credentials:'private-secret'};
  const h=harness(t,{fast:(body,index)=>index===1?shopping:modelNeeded,model:()=>response(malicious)});h.open();await h.ask('Show me silver bunnies');const card=h.root.querySelector('.card'),url=h.win.location.href;await h.ask('What makes something meaningful?');
  const call=h.network.find(value=>value.url.pathname==='/api/concierge-demo-turn');assert.equal(call.body.action,'conversation');assert.equal(call.body.message,'What makes something meaningful?');assert.deepEqual(Object.keys(call.body).sort(),['action','history','message','publicContext']);assert.deepEqual(call.body.publicContext,{pageKind:'home',hasSelection:true,progress:'selection-shown'});
  assert.ok(call.body.history.length<=6);assert.ok(call.body.history.every(item=>Object.keys(item).sort().join(',')==='content,role'&&item.content.length<=300));assert.doesNotMatch(JSON.stringify(call.body),/credentials|private-secret|variantId|accountId/);
  assert.equal(h.root.querySelector('.card'),card);assert.deepEqual(h.snapshot().preferences,preferences);assert.deepEqual(h.snapshot().meanings,[meaning]);assert.equal(h.root.querySelector('.caption-text').textContent,malicious.reply);assert.doesNotMatch(h.root.textContent,/Unverified intruder|Invented history|Buy now/);assert.equal(h.win.location.href,url);assert.equal(h.win.sessionStorage.getItem('brites-sandbox-cart'),null);assert.equal(h.root.querySelector('.review'),null);
});

test('model conversation message is capped at six hundred characters and previous turns at three hundred',async t=>{
  const longReply='r'.repeat(850),h=harness(t,{fast:(body,index)=>index===1?{...shopping,reply:longReply}:modelNeeded});h.open();await h.ask('Show me silver bunnies');await h.ask('Tell me '.repeat(100));const call=h.network.find(value=>value.url.pathname==='/api/concierge-demo-turn');assert.equal(call.body.message.length,600);assert.equal(call.body.history.at(-1).content.length,300);assert.equal(call.body.history.at(-1).role,'assistant');
});

for(const body of [{conversationOnly:false,reply:'Wrong route reply'},{conversationOnly:true,reply:'x'.repeat(1001)},{conversationOnly:true,reply:{text:'Wrong reply type'}}])test('invalid model conversation envelope keeps the safe conversational fallback',async t=>{
  const h=harness(t,{fast:(value,index)=>index===1?shopping:modelNeeded,model:()=>response(body)});h.open();await h.ask('Show bunnies');await h.ask('Tell me something interesting');assert.equal(h.root.querySelector('.caption-text').textContent,modelNeeded.reply);assert.equal(h.root.querySelectorAll('.card').length,1);assert.match(h.root.querySelector('.status').textContent,/temporarily unavailable/);assert.equal(h.snapshot().preferences.budget,70);
});

test('provider failure keeps the selection and a usable composer',async t=>{
  const h=harness(t,{fast:(value,index)=>index===1?shopping:modelNeeded,model:()=>{throw Error('Synthetic provider unavailable');}});h.open();await h.ask('Show bunnies');await h.ask('How do you choose your favourite?');assert.equal(h.root.querySelectorAll('.card').length,1);assert.equal(h.root.querySelector('.caption-text').textContent,modelNeeded.reply);assert.equal(h.root.querySelector('input[aria-label="Message the gift concierge"]').disabled,false);assert.match(h.root.querySelector('.status').textContent,/temporarily unavailable/);
});

test('closing during model conversation aborts the turn and ignores an eventual late reply',async t=>{
  const pending=deferred(),h=harness(t,{fast:(value,index)=>index===1?shopping:modelNeeded,model:()=>pending.promise});h.open();await h.ask('Show bunnies');await h.ask('Tell me a little story');const call=h.network.find(value=>value.url.pathname==='/api/concierge-demo-turn');assert.ok(call);h.close();await settle();assert.equal(call.signal.aborted,true);const before=h.snapshot().history;pending.resolve(response({conversationOnly:true,reply:'Obsolete reply must never appear',aiUsed:true}));await settle();assert.deepEqual(h.snapshot().history,before);assert.doesNotMatch(h.root.textContent,/Obsolete reply must never appear/);assert.equal(h.root.querySelector('.panel').hidden,true);
});

test('Start fresh cancels a pending conversation and prevents its reply from replacing a newer selection',async t=>{
  const pending=deferred(),h=harness(t,{fast:(value,index)=>index===2?modelNeeded:shopping,model:()=>pending.promise});h.open();await h.ask('Show bunnies');await h.ask('Tell me a story');const call=h.network.find(value=>value.url.pathname==='/api/concierge-demo-turn');h.button('Start fresh').click();await settle();assert.equal(call.signal.aborted,true);await h.ask('Find a graduation bunny');pending.resolve(response({conversationOnly:true,reply:'Obsolete story',aiUsed:true}));await settle();assert.equal(h.snapshot().latestReply,shopping.reply);assert.equal(h.root.querySelectorAll('.card').length,1);assert.doesNotMatch(h.root.textContent,/Obsolete story/);
});

test('hiding during model conversation aborts the turn and blocks a late provider response',async t=>{
  const pending=deferred(),h=harness(t,{fast:(value,index)=>index===1?shopping:modelNeeded,model:()=>pending.promise});h.open();await h.ask('Show bunnies');await h.ask('Tell me why people choose symbols');const call=h.network.find(value=>value.url.pathname==='/api/concierge-demo-turn');h.hide(true);await settle();assert.equal(call.signal.aborted,true);const history=h.snapshot().history;pending.resolve(response({conversationOnly:true,reply:'Late hidden-page reply',aiUsed:true}));await settle();assert.deepEqual(h.snapshot().history,history);assert.doesNotMatch(h.root.textContent,/Late hidden-page reply/);h.hide(false);assert.equal(h.root.querySelector('input[aria-label="Message the gift concierge"]').disabled,false);
});

test('product pointer attention focuses and clears the avatar without a request, navigation or bag mutation',async t=>{
  const h=harness(t);h.open();await h.ask('Show bunnies');const card=h.root.querySelector('.card'),calls=h.network.length,url=h.win.location.href,before=h.avatar.focus.length;h.root.querySelector('.concierge-avatar-stage').getBoundingClientRect=()=>({left:10,top:20,width:200,height:100});card.getBoundingClientRect=()=>({left:310,top:240,width:80,height:80});card.dispatchEvent(new h.win.Event('pointerenter'));assert.equal(h.avatar.focus.length,before+1);assert.equal(card.dataset.attentive,'true');const target=h.avatar.focus.at(-1);assert.deepEqual(target,{x:1,y:-1},'far product card uses a bounded directional target rather than unbounded pose geometry');const clears=h.avatar.clear;card.dispatchEvent(new h.win.Event('pointerleave'));assert.equal(h.avatar.clear,clears+1);assert.equal(card.dataset.attentive,undefined);assert.equal(h.network.length,calls);assert.equal(h.win.location.href,url);assert.equal(h.win.sessionStorage.getItem('brites-sandbox-cart'),null);
});

test('keyboard attention stays inside a card then clears when focus leaves, without a request',async t=>{
  const h=harness(t);h.open();await h.ask('Show bunnies');const card=h.root.querySelector('.card'),calls=h.network.length,link=card.querySelector('a'),choose=card.querySelector('.choose');card.dispatchEvent(new h.win.FocusEvent('focusin',{bubbles:true,relatedTarget:null}));assert.equal(card.dataset.attentive,'true');const clears=h.avatar.clear;card.dispatchEvent(new h.win.FocusEvent('focusout',{bubbles:true,relatedTarget:choose}));assert.equal(h.avatar.clear,clears);card.dispatchEvent(new h.win.FocusEvent('focusout',{bubbles:true,relatedTarget:h.win.document.querySelector('#shop')}));assert.equal(h.avatar.clear,clears+1);assert.equal(h.network.length,calls);assert.ok(link);
});

test('hidden or dismissed guide ignores later card pointer attention',async t=>{
  const h=harness(t);h.open();await h.ask('Show bunnies');const card=h.root.querySelector('.card');h.hide(true);let focus=h.avatar.focus.length;card.dispatchEvent(new h.win.Event('pointerenter'));assert.equal(h.avatar.focus.length,focus);h.hide(false);h.close();focus=h.avatar.focus.length;card.dispatchEvent(new h.win.Event('pointerenter'));assert.equal(h.avatar.focus.length,focus);assert.equal(h.win.sessionStorage.getItem('brites-sandbox-cart'),null);
});

test('requested options updates the exact selected public context and cues product attention without a cart request',async t=>{
  const h=harness(t,{url:'https://growth-sandbox.example/concierge-sandbox.html?product=bunny-1'});h.open();await h.ask('Show bunnies');await h.activateVoice();const card=h.root.querySelector('.card'),calls=h.network.length;card.querySelector('.choose').click();const context=h.contexts.at(-1);assert.equal(context.pageKind,'product');assert.equal(context.currentHandle,piece.handle);assert.equal(context.selectedHandle,piece.handle);assert.deepEqual(context.displayedPieces,[{id:piece.id,handle:piece.handle,title:piece.title}]);assert.equal(card.dataset.attentive,'true');assert.ok(card.querySelector('.options'));assert.equal(h.root.querySelector('.review'),null);assert.equal(h.network.length,calls);assert.equal(h.win.sessionStorage.getItem('brites-sandbox-cart'),null);
});
