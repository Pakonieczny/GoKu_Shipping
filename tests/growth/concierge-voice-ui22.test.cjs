'use strict';
// DOM integration contracts only: actual voice quality, microphone hardware,
// WebGL motion and screen geometry require the separate deployed browser check.
const test=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const {JSDOM,VirtualConsole}=require('jsdom');
const source=fs.readFileSync(require.resolve('../../brites-concierge.js'),'utf8');
const tick=()=>new Promise(resolve=>setImmediate(resolve));
async function settle(){await tick();await tick();}
function deferred(){let resolve,reject;const promise=new Promise((a,b)=>{resolve=a;reject=b;});return {promise,resolve,reject};}
const variant={id:'gid://shopify/ProductVariant/101',numericId:'101',title:'Sterling Silver',price:54,available:true,options:[{name:'Metal',value:'Sterling Silver'}]};
const product={id:'gid://shopify/Product/1',handle:'bunny-1',url:'https://britesjewelry.com/products/bunny-1',title:'Bunny Necklace',type:'Necklace',currency:'USD',variants:[variant],variantsComplete:true,suggestedVariantId:variant.id,minPrice:54,why:'A personal symbol for your milestone.'};
const answer={reply:'DETERMINISTIC RETRIEVAL SUMMARY',preferences:{},products:[product],meanings:[]};
function fixture(t,options={}){
  const errors=[],console=new VirtualConsole();console.on('jsdomError',e=>errors.push(e));
  const dom=new JSDOM('<!doctype html><html><body><button id="ordinary">Browse</button></body></html>',{url:'https://growth-sandbox.example/concierge-sandbox.html',pretendToBeVisual:true,runScripts:'outside-only',virtualConsole:console});
  const w=dom.window,d=w.document,script=d.createElement('script');script.src='https://growth-sandbox.example/brites-concierge.js';script.dataset.sandbox='true';Object.defineProperty(d,'currentScript',{get:()=>script});
  let hidden=false,voiceConfig;Object.defineProperty(d,'hidden',{get:()=>hidden});
  const calls={start:0,stop:0,interrupt:0,dispose:0,states:[],levels:[],paused:[],requests:[],signals:[]};
  if(options.timers){const set=w.setTimeout.bind(w),clear=w.clearTimeout.bind(w);w.setTimeout=(fn,ms)=>{if(ms===11000){const id=100000+options.timers.length;options.timers.push({fn,ms,id});return id;}return set(fn,ms);};w.clearTimeout=id=>{if(id<100000)clear(id);};}
  w.speechSynthesis={speak(){throw Error('Browser synthesis must never run');}};
  w.BritesConciergeAvatar={create(){return {setState:v=>calls.states.push(v),setVisible(){},retry(){},setEmotion(){},setPaused:v=>calls.paused.push(v),setLevel:v=>calls.levels.push(v),triggerGreeting(){},destroy(){}};}};
  w.BritesConciergeVoice={create(config){voiceConfig=config;return {start:async()=>{calls.start++;return options.start?options.start(calls.start):true;},stop:async()=>{calls.stop++;},interrupt(){calls.interrupt++;},dispose:async()=>{calls.dispose++;}};}};
  w.fetch=async(url,init={})=>{const body=init.body?JSON.parse(init.body):{};calls.requests.push(body);calls.signals.push(init.signal);return {ok:true,json:async()=>body.message?(options.answerFor?await options.answerFor(body):(options.answer||answer)):{}};};
  w.HTMLElement.prototype.scrollIntoView=function(){};w.eval(source);
  const root=d.querySelector('brites-concierge').shadowRoot;
  const button=label=>[...root.querySelectorAll('button')].find(n=>n.textContent.trim()===label||n.getAttribute('aria-label')===label);
  t.after(()=>{try{w.BritesConcierge.close();}catch{}w.close();});
  async function activate(){button('Talk to me').click();await settle();root.querySelector('script[src$="brites-concierge-voice.js"]')?.dispatchEvent(new w.Event('load'));await settle();}
  return {w,d,root,button,calls,errors,activate,open:()=>w.BritesConcierge.open(),get config(){return voiceConfig;},saved:()=>JSON.parse(w.sessionStorage.getItem('brites-concierge-v1')),hide(v){hidden=v;d.dispatchEvent(new w.Event('visibilitychange'));}};
}
test('voice is the initial focus while optional captions, typing and history remain independent',async t=>{
  const h=fixture(t);h.open();await settle();assert.equal(h.root.activeElement,h.button('Talk to me'));assert.equal(h.root.querySelector('.composer').hidden,true);assert.equal(h.root.querySelector('.messages').hidden,true);assert.equal(h.root.querySelector('.captions').hidden,false);
  h.button('Type instead').click();assert.equal(h.root.querySelector('.composer').hidden,false);assert.equal(h.root.activeElement,h.root.querySelector('input'));assert.equal(h.button('Hide typing').getAttribute('aria-expanded'),'true');
  h.button('History').click();assert.equal(h.root.querySelector('.messages').hidden,false);assert.equal(h.button('History').getAttribute('aria-expanded'),'true');
  h.button('Captions').click();assert.equal(h.root.querySelector('.captions').hidden,true);assert.equal(h.button('Captions').getAttribute('aria-pressed'),'false');assert.equal(h.root.querySelector('.messages').hidden,false);assert.equal(h.calls.start,0);assert.equal(h.button('Read aloud'),undefined);
});
test('native caption deltas stream immediately; final item is saved once and full text remains available',async t=>{
  const h=fixture(t);h.open();await h.activate();const transcript={role:'assistant',itemId:'output-1'};
  h.config.onTranscript({...transcript,delta:'A little ',final:false});h.config.onTranscript({...transcript,delta:'meaning for you.',final:false});assert.equal(h.root.querySelector('.caption-text').textContent,'A little meaning for you.');assert.ok(!h.saved().history.some(m=>m.content==='A little meaning for you.'));
  const full=('A little meaning for you. '+('This is an optional explanation with room to read. '.repeat(30))).trim();h.config.onTranscript({...transcript,text:full,final:true});h.config.onTranscript({...transcript,text:full,final:true});
  assert.equal(h.saved().history.filter(m=>m.content===full).length,1);assert.equal(h.root.querySelector('.caption-text').textContent,full);assert.equal([...h.root.querySelectorAll('.bubble')].filter(n=>n.textContent===full).length,1);assert.match(h.root.querySelector('.caption-label').textContent,/AI/);
});
test('retrieval presents real product cards without replacing native wording or duplicating the user turn',async t=>{
  const h=fixture(t);h.open();await h.activate();h.config.onTranscript({role:'user',itemId:'input-1',text:'A bunny for graduation',final:true});const result=await h.config.onTool({message:'A bunny for graduation'});
  assert.equal(result.products[0].title,product.title);assert.equal(h.root.querySelector('.product-tray').hidden,false);assert.equal(h.root.querySelector('.card').closest('.selection-scroll'),h.root.querySelector('.selection-scroll'));assert.equal(h.root.querySelector('.messages .card'),null);assert.doesNotMatch(h.root.querySelector('.messages').textContent,/DETERMINISTIC RETRIEVAL SUMMARY/);
  h.config.onTranscript({role:'assistant',itemId:'output-1',delta:'What a lovely milestone. ',final:false});h.config.onTranscript({role:'assistant',itemId:'output-1',text:'What a lovely milestone. A bunny could be your personal reminder of a fresh start.',final:true});
  assert.equal(h.saved().history.filter(m=>m.role==='user').length,1);assert.equal(h.saved().history.filter(m=>m.role==='assistant').length,1);assert.doesNotMatch(h.saved().history.map(m=>m.content).join(' '),/DETERMINISTIC RETRIEVAL SUMMARY/);assert.equal(h.w.sessionStorage.getItem('brites-sandbox-cart'),null);
});
test('animation pause keeps conversation connected; interrupt is shown for actual speaking state',async t=>{
  const h=fixture(t);h.open();await h.activate();const before=h.calls.stop;h.button('Pause animation').click();assert.equal(h.calls.paused.at(-1),true);assert.equal(h.calls.stop,before);assert.equal(h.button('End voice').getAttribute('aria-pressed'),'true');
  h.config.onState('speaking');h.config.onLevel({input:.1,output:.7});assert.equal(h.button('Let me speak').hidden,false);assert.equal(h.calls.states.at(-1),'speaking');assert.equal(h.calls.levels.at(-1),.7);h.button('Let me speak').click();assert.equal(h.calls.interrupt,1);assert.equal(h.calls.states.at(-1),'listening');assert.equal(h.button('Let me speak').hidden,true);
  h.config.onStopped();assert.equal(h.button('Talk to me').disabled,false);assert.equal(h.button('Talk to me').getAttribute('aria-pressed'),'false');assert.equal(h.calls.levels.at(-1),0);
});
test('ending voice ignores late playback/caption callbacks while the panel stays open',async t=>{
  const h=fixture(t);h.open();await h.activate();h.config.onState('speaking');h.button('End voice').click();const states=h.calls.states.length,caption=h.root.querySelector('.caption-text').textContent;
  h.config.onState('speaking');h.config.onLevel({output:1});h.config.onTranscript({role:'assistant',itemId:'late',text:'Late reply',final:true});assert.equal(h.calls.states.length,states);assert.equal(h.calls.levels.at(-1),0);assert.equal(h.root.querySelector('.caption-text').textContent,caption);assert.ok(!h.saved().history.some(m=>m.content==='Late reply'));
});
for(const end of ['dismiss','hidden'])test('late successful native connection after '+end+' is stopped and cannot reopen microphone UI',async t=>{
  const start=deferred(),h=fixture(t,{start:()=>start.promise});h.open();await h.activate();assert.equal(h.calls.start,1);if(end==='dismiss')h.w.BritesConcierge.close();else h.hide(true);start.resolve(true);await settle();assert.ok(h.calls.stop>0);assert.equal(h.button('Talk to me').getAttribute('aria-pressed'),'false');assert.equal(h.button('Talk to me').disabled,false);assert.equal(h.button('Let me speak').hidden,true);
});
test('pagehide ends native audio and pageshow restores only the animation without reconnecting voice',async t=>{
  const h=fixture(t);h.open();await h.activate();const before=h.calls.stop;h.w.dispatchEvent(new h.w.PageTransitionEvent('pagehide',{persisted:true}));await settle();assert.ok(h.calls.stop>before);assert.equal(h.calls.dispose,1);assert.equal(h.button('Talk to me').getAttribute('aria-pressed'),'false');h.w.dispatchEvent(new h.w.PageTransitionEvent('pageshow',{persisted:true}));await settle();assert.equal(h.calls.start,1);
});


test('connecting remains attentive and cancellable; late prior connection cannot close a newer session',async t=>{
  const first=deferred(),h=fixture(t,{start:count=>count===1?first.promise:true});h.open();await h.activate();assert.equal(h.button('Cancel connection').disabled,false);h.config.onState('connecting');assert.equal(h.root.querySelector('.voice-state').textContent,'Connecting your voice…');assert.equal(h.calls.states.at(-1),'thinking');
  h.button('Cancel connection').click();assert.equal(h.button('Talk to me').disabled,false);const stops=h.calls.stop;await h.activate();assert.equal(h.button('End voice').getAttribute('aria-pressed'),'true');first.resolve(true);await settle();assert.equal(h.calls.stop,stops);assert.equal(h.button('End voice').getAttribute('aria-pressed'),'true');
});
test('VAD abort unlocks immediately and a provider that ignores abort cannot save stale preferences or cards',async t=>{
  const reply=deferred(),h=fixture(t,{answerFor:()=>reply.promise});h.open();await h.activate();const abort=new h.w.AbortController();const lookup=h.config.onTool({message:'A former preference'},{signal:abort.signal});await settle();assert.equal(h.button('Send').disabled,true);const fetchSignal=h.calls.signals.find(Boolean);assert.equal(fetchSignal.aborted,false);
  abort.abort();assert.equal(fetchSignal.aborted,true);assert.equal(h.button('Send').disabled,false);assert.equal(h.root.querySelector('input').disabled,false);assert.match((await lookup).error,/cancelled/);const beforeStatus=h.root.querySelector('.status').textContent;
  reply.resolve({...answer,preferences:{query:'STALE'}});await settle();assert.equal(h.root.querySelectorAll('.card').length,0);assert.equal(h.saved().preferences.query,undefined);assert.equal(h.root.querySelector('.status').textContent,beforeStatus);assert.equal(h.saved().pendingTurn,null);
});
for(const end of ['End voice','hidden'])test(end+' cancels lookup and cannot poison or unlock a newer request',async t=>{
  const old=deferred(),fresh=deferred(),h=fixture(t,{answerFor:body=>body.message==='Old preference'?old.promise:fresh.promise});h.open();await h.activate();const stale=h.config.onTool({message:'Old preference'});await settle();if(end==='End voice')h.button('End voice').click();else{h.hide(true);h.hide(false);}assert.equal(h.button('Send').disabled,false);assert.match((await stale).error,/cancelled/);await h.activate();const current=h.config.onTool({message:'New preference'});await settle();assert.equal(h.button('Send').disabled,true);
  old.resolve({...answer,preferences:{query:'STALE'},reply:'STALE reply'});await settle();assert.equal(h.button('Send').disabled,true);assert.equal(h.root.querySelectorAll('.card').length,0);assert.doesNotMatch(h.root.querySelector('.caption-text').textContent,/STALE/);
  fresh.resolve({...answer,preferences:{query:'CURRENT'}});const checked=await current;assert.equal(checked.products[0].title,product.title);assert.equal(h.saved().preferences.query,'CURRENT');assert.equal(h.button('Send').disabled,false);assert.equal(h.root.querySelectorAll('.card').length,1);
});
test('voice retrieval deadline expires before adapter deadline and releases a non-responsive request',async t=>{
  const reply=deferred(),timers=[],h=fixture(t,{timers,answerFor:()=>reply.promise});h.open();await h.activate();const lookup=h.config.onTool({message:'A stalled request'});await settle();assert.equal(timers.length,1);assert.ok(timers[0].ms<14000);timers[0].fn();const result=await lookup;assert.match(result.error,/could not be checked/);assert.equal(h.button('Send').disabled,false);assert.equal(h.calls.signals.find(Boolean).aborted,true);reply.resolve({...answer,preferences:{query:'STALE'}});await settle();assert.equal(h.saved().preferences.query,undefined);assert.equal(h.root.querySelectorAll('.card').length,0);
});


for(const phase of ['active','connecting'])test('Type instead ends '+phase+' voice before opening the silent keyboard path',async t=>{
  const pending=deferred(),h=fixture(t,{start:()=>phase==='connecting'?pending.promise:true});h.open();await h.activate();const stops=h.calls.stop;h.button('Type instead').click();assert.ok(h.calls.stop>stops);assert.equal(h.button('Talk to me').getAttribute('aria-pressed'),'false');assert.equal(h.root.querySelector('.composer').hidden,false);assert.equal(h.root.activeElement,h.root.querySelector('input'));assert.equal(h.root.querySelector('input').disabled,false);if(phase==='connecting'){pending.resolve(true);await settle();assert.equal(h.button('Talk to me').getAttribute('aria-pressed'),'false');}
  h.button('Hide typing').click();assert.equal(h.calls.stop,stops+1,'closing the keyboard does not issue an additional voice stop');
});
test('typed submission defensively ends a subsequently restarted voice session',async t=>{
  const h=fixture(t);h.open();h.button('Type instead').click();await h.activate();assert.equal(h.button('End voice').getAttribute('aria-pressed'),'true');const stops=h.calls.stop,input=h.root.querySelector('input');input.value='A bunny necklace';h.root.querySelector('form').dispatchEvent(new h.w.Event('submit',{bubbles:true,cancelable:true}));await settle();assert.ok(h.calls.stop>stops);assert.equal(h.button('Talk to me').getAttribute('aria-pressed'),'false');assert.equal(h.root.querySelector('.composer').hidden,false);assert.equal(input.disabled,false);assert.equal(h.root.querySelectorAll('.card').length,1);
});
