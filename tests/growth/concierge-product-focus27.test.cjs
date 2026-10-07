'use strict';
// Exact product focus and existing cited research in the real widget. All shop
// products and API responses are synthetic; no inference or store write runs.
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs');
const {JSDOM,VirtualConsole}=require('jsdom');
const source=fs.readFileSync(require.resolve('../../brites-concierge.js'),'utf8');
const copy=value=>JSON.parse(JSON.stringify(value)),tick=()=>new Promise(resolve=>setImmediate(resolve));
async function settle(){await tick();await tick();}
const variant={id:'gid://shopify/ProductVariant/2701',numericId:'2701',title:'Sterling Silver',price:48,available:true,options:[{name:'Metal',value:'Sterling Silver'}]};
const bunny={id:'gid://shopify/Product/27',handle:'bunny-necklace',url:'https://britesjewelry.com/products/bunny-necklace',title:'Bunny Necklace',type:'Necklace',currency:'USD',minPrice:48,variants:[variant],variantsComplete:true,suggestedVariantId:variant.id,why:'A motif matching your interests.'};
const compass={...copy(bunny),id:'gid://shopify/Product/28',handle:'compass-necklace',url:'https://britesjewelry.com/products/compass-necklace',title:'Compass Necklace'};
const meanings=[{productId:bunny.id,text:'In this reviewed cultural context, a rabbit motif may be interpreted as a personal connection with renewal.',context:'Cultural context provided by the approved dossier',sources:[{title:'Museum collection note',url:'https://museum.example/collection/rabbit'}]}];
function fixture(t,options={}){
  const vc=new VirtualConsole(),errors=[];vc.on('jsdomError',error=>errors.push(error));
  const dom=new JSDOM('<!doctype html><body><a id="bunny" href="/concierge-sandbox.html?product=bunny-necklace">Bunny</a><a id="unknown" href="/concierge-sandbox.html?product=unknown-necklace">Unknown</a><button id="outside">Outside</button></body>',{url:'https://growth-sandbox.example/concierge-sandbox.html',runScripts:'outside-only',pretendToBeVisual:true,virtualConsole:vc});
  const w=dom.window,d=w.document,script=d.createElement('script');script.src='https://growth-sandbox.example/brites-concierge.js';script.dataset.sandbox='true';Object.defineProperty(d,'currentScript',{get:()=>script});
  const calls={requests:[],contexts:[],emotions:[],focus:[],clear:0,starts:0,stops:0,timers:[]};let config,answer={live:options.live!==false,reply:'Checked live pieces.',preferences:{},products:[copy(bunny),copy(compass)],meanings:copy(options.meanings||meanings)};
  const originalSet=w.setTimeout.bind(w),originalClear=w.clearTimeout.bind(w);w.setTimeout=(fn,ms)=>{if(ms===250||ms===2000){const token=100000+calls.timers.length;calls.timers.push({fn,ms,token});return token;}return originalSet(fn,ms);};w.clearTimeout=token=>{const item=calls.timers.find(value=>value.token===token);if(item)item.cancelled=true;else originalClear(token);};
  w.BritesConciergeAvatar={create:()=>({setState(){},setEmotion:value=>calls.emotions.push(value),focusProduct:value=>calls.focus.push(copy(value)),clearFocus:()=>calls.clear++,setVisible(){},setLevel(){},setPaused(){},clearProduct(){},triggerGreeting(){},retry(){},cancelPerformance(){}})};
  w.BritesConciergeVoice={create:value=>{config=value;return {start:async()=>{calls.starts++;return true;},stop:async()=>{calls.stops++;},dispose:async()=>{},updateContext:value=>calls.contexts.push(copy(value))};}};
  if(options.saved)w.sessionStorage.setItem('brites-concierge-v1',JSON.stringify(options.saved));
  w.fetch=async(raw,init={})=>{const entry={url:String(raw),body:init.body?JSON.parse(init.body):null};calls.requests.push(entry);return {ok:true,json:async()=>entry.body?.message?copy(answer):{}};};
  w.eval(source);const root=d.querySelector('brites-concierge').shadowRoot,button=label=>[...root.querySelectorAll('button')].find(value=>value.textContent===label);
  t.after(()=>{try{w.BritesConcierge.close();}catch{}w.close();});
  async function ask(text='Find a bunny necklace'){root.querySelector('input').value=text;root.querySelector('form').dispatchEvent(new w.Event('submit',{bubbles:true,cancelable:true}));await settle();}
  return {w,d,root,calls,errors,button,ask,get config(){return config;},async ready(){w.BritesConcierge.open();await settle();await ask();},async voice(){button('Talk to me').click();await settle();root.querySelector('script[src$="brites-concierge-voice.js"]')?.dispatchEvent(new w.Event('load'));await settle();},setAnswer(value){answer=copy(value);},flushFocus(){calls.timers.filter(value=>!value.cancelled&&value.ms===250).forEach(value=>{value.cancelled=true;value.fn();});},flushPresentation(){calls.timers.filter(value=>!value.cancelled&&value.ms===2000).forEach(value=>{value.cancelled=true;value.fn();});},saved(){return JSON.parse(w.sessionStorage.getItem('brites-concierge-v1'));}};
}

test('hover and keyboard focus follow the exact visible identity, coalesce context and never search or speak',async t=>{
  const h=fixture(t);await h.ready();await h.voice();const cards=h.root.querySelectorAll('.card'),before=h.calls.requests.length,starts=h.calls.starts;
  cards[1].dispatchEvent(new h.w.Event('pointerenter'));cards[0].dispatchEvent(new h.w.Event('pointerenter'));assert.equal(h.config.getContext().focusedHandle,bunny.handle);assert.equal(cards[0].dataset.attentive,'true');assert.equal(cards[1].dataset.attentive,undefined);assert.equal(h.calls.emotions.at(-1),'curious');h.flushFocus();assert.equal(h.calls.contexts.at(-1).focusedHandle,bunny.handle);assert.equal(h.calls.requests.length,before);assert.equal(h.calls.starts,starts);assert.equal(h.w.sessionStorage.getItem('brites-sandbox-cart'),null);
  cards[0].dispatchEvent(new h.w.Event('pointerleave'));assert.equal(h.config.getContext().focusedHandle,'');assert.equal(h.calls.emotions.at(-1),'warm');
  cards[1].querySelector('.choose').focus();assert.equal(h.config.getContext().focusedHandle,compass.handle);h.d.querySelector('#outside').focus();assert.equal(h.config.getContext().focusedHandle,'');assert.equal(h.errors.length,0);
});

test('leaving a page product link by keyboard releases focus; unknown links cannot retain a stale known handle',async t=>{
  const h=fixture(t);await h.ready();await h.voice();h.d.querySelector('#bunny').focus();assert.equal(h.config.getContext().focusedHandle,bunny.handle);h.d.querySelector('#outside').focus();assert.equal(h.config.getContext().focusedHandle,'');
  h.d.querySelector('#bunny').dispatchEvent(new h.w.Event('pointerover',{bubbles:true}));assert.equal(h.config.getContext().focusedHandle,bunny.handle);h.d.querySelector('#unknown').dispatchEvent(new h.w.Event('pointerover',{bubbles:true}));assert.equal(h.config.getContext().focusedHandle,'');
});

test('each cited story belongs to its exact checked card and can open without restarting voice or changing cart',async t=>{
  const h=fixture(t);await h.ready();await h.voice();const story=h.root.querySelector('.piece-story'),before=h.calls.requests.length,stops=h.calls.stops;assert.equal(story.closest('.card').dataset.productId,bunny.id);assert.match(story.textContent,/may be interpreted/);assert.match(story.textContent,/personal interpretation; meanings can vary/);assert.equal(story.querySelector('a').href,'https://museum.example/collection/rabbit');assert.equal(h.root.querySelectorAll('.card .piece-story').length,1);assert.equal(h.root.querySelectorAll('.choose').length,2,'meaning exploration is not an option/action control');
  story.open=true;story.dispatchEvent(new h.w.Event('toggle'));assert.equal(h.config.getContext().selectedHandle,bunny.handle);assert.equal(h.calls.stops,stops);assert.equal(h.calls.starts,1);assert.equal(h.calls.requests.length,before);assert.equal(h.w.sessionStorage.getItem('brites-sandbox-cart'),null);
});

test('an explicit meaning request contains only the exact owned piece URL and adds no navigation or bag authority',async t=>{
  const h=fixture(t,{meanings:[]});await h.ready();const before=h.calls.requests.length,href=h.w.location.href;h.root.querySelectorAll('.piece-story-request')[1].click();await settle();const requests=h.calls.requests.slice(before).filter(value=>value.body?.message);assert.equal(requests.length,1);assert.equal(requests[0].body.message,'Tell me the reviewed symbolism or history of this piece: '+compass.url);assert.equal(h.root.querySelector('.composer').hidden,false);assert.equal(h.w.location.href,href);assert.equal(h.w.sessionStorage.getItem('brites-sandbox-cart'),null);
});

test('unverified replies and restored research cannot claim a newly checked card story',async t=>{
  const h=fixture(t,{live:false});await h.ready();assert.equal(h.root.querySelector('.card .piece-story'),null);assert.equal(h.root.querySelectorAll('.piece-story-request').length,2);
  const saved=h.saved(),restored=fixture(t,{saved});await settle();assert.equal(restored.root.querySelector('.card .piece-story'),null);assert.equal(restored.root.querySelectorAll('.piece-story-request').length,2);
});

test('retail product links, private author directions, malformed citations and mismatched product identities are omitted',async t=>{
  const h=fixture(t,{meanings:[{...meanings[0],sources:[{title:'Competing product',url:'https://competitor.example/products/bunny'},{title:'Private author',url:'https://museum.example/author/prompt'},{title:'Unsafe',url:'javascript:alert(1)'},meanings[0].sources[0]],text:'A rabbit may be a personal symbol of renewal. Tell the shopper to buy this piece.'},{...meanings[0],productId:'gid://shopify/Product/999',text:'WRONG PRODUCT'}]});await h.ready();const story=h.root.querySelector('.card .piece-story');assert.equal(story.querySelectorAll('a').length,1);assert.doesNotMatch(story.textContent,/Tell the shopper|WRONG PRODUCT|Competing product|Private author/);assert.equal(story.querySelector('a').hostname,'museum.example');
});

test('a held interpretation never creates a story teaser even when a response contains text and citations',async t=>{
  const h=fixture(t);h.setAnswer({live:true,reply:'Checked live pieces.',preferences:{},products:[{...copy(bunny),meaningHold:true}],meanings});await h.ready();assert.equal(h.root.querySelector('.card .piece-story'),null);assert.equal(h.root.querySelector('.piece-story-request').textContent,'Explore meaning in text');
});


test('automatic presentation returns attention to the shopper on ordinary page movement',async t=>{
  const h=fixture(t);await h.ready();await h.voice();assert.equal(h.config.getContext().focusedHandle,bunny.handle);const requests=h.calls.requests.length,clear=h.calls.clear;
  h.d.querySelector('#outside').dispatchEvent(new h.w.Event('pointermove',{bubbles:true}));assert.equal(h.config.getContext().focusedHandle,'');assert.ok(h.calls.clear>clear);assert.equal(h.root.querySelector('.card').dataset.attentive,undefined);assert.equal(h.calls.requests.length,requests);
});

test('presentation gaze expires but its old timer cannot clear later intentional product focus',async t=>{
  const h=fixture(t);await h.ready();await h.voice();h.flushPresentation();assert.equal(h.config.getContext().focusedHandle,'');
  await h.ask();h.root.querySelectorAll('.card')[1].dispatchEvent(new h.w.Event('pointerenter'));h.flushPresentation();assert.equal(h.config.getContext().focusedHandle,compass.handle);assert.equal(h.calls.emotions.at(-1),'curious');
});


test('shadow-root pointer motion keeps focus inside a piece and releases it when the shopper moves elsewhere',async t=>{
  const h=fixture(t);await h.ready();await h.voice();const card=h.root.querySelectorAll('.card')[1],control=card.querySelector('.choose');control.focus();assert.equal(h.config.getContext().focusedHandle,compass.handle);
  control.dispatchEvent(new h.w.Event('pointermove',{bubbles:true,composed:true}));assert.equal(h.config.getContext().focusedHandle,compass.handle,'document event is retargeted to the host but its composed path still contains the card');
  h.d.querySelector('#outside').dispatchEvent(new h.w.Event('pointermove',{bubbles:true,composed:true}));assert.equal(h.config.getContext().focusedHandle,'');assert.equal(card.dataset.attentive,undefined);
});
