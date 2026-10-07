'use strict';
// Actual widget + production expression planner, with synthetic native events.
// These regressions do not establish microphone, word-alignment, GPU or trust quality.
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs');
const {JSDOM,VirtualConsole}=require('jsdom');
const expressionApi=require('../../brites-concierge-expression.js');
const widget=fs.readFileSync(require.resolve('../../brites-concierge.js'),'utf8');
const actions=fs.readFileSync(require.resolve('../../brites-concierge-voice-actions.js'),'utf8');
const tick=()=>new Promise(resolve=>setImmediate(resolve));
async function settle(){await tick();await tick();}
const product={id:'gid://shopify/Product/1',handle:'compass-necklace',url:'https://britesjewelry.com/products/compass-necklace',title:'Compass Necklace',currency:'USD',type:'Necklace',variantsComplete:true,minPrice:54,suggestedVariantId:'gid://shopify/ProductVariant/101',variants:[{id:'gid://shopify/ProductVariant/101',numericId:'101',title:'Sterling Silver / 18 inch',price:54,available:true,options:[{name:'Metal',value:'Sterling Silver'},{name:'Length',value:'18 inch'}]}]};

// Fixture copied from concierge-expression-integration28, with a controlled
// delayed script-load hook and final user transcript hook for integration checks.
function fixture(t,{moduleAvailable=true}={}){
  const errors=[],vc=new VirtualConsole();vc.on('jsdomError',value=>errors.push(value));
  const dom=new JSDOM('<!doctype html><body></body>',{url:'https://preview.example/concierge-sandbox.html',pretendToBeVisual:true,runScripts:'outside-only',virtualConsole:vc}),w=dom.window,d=w.document;
  const script=d.createElement('script');script.src='/brites-concierge.js';script.dataset.sandbox='true';Object.defineProperty(d,'currentScript',{get:()=>script});
  let hidden=false,clock=1791340000000,serial=0,controller=null;Object.defineProperty(d,'hidden',{get:()=>hidden});w.Date.now=()=>clock;
  const timers=new Map(),calls={expressions:[],emotions:[],levels:[],states:[],performances:[],requests:[],starts:0,stops:0,interrupts:0};
  w.setTimeout=(fn,ms)=>{const id=++serial;timers.set(id,{fn,at:clock+Number(ms||0)});return id;};w.clearTimeout=id=>timers.delete(id);
  w.HTMLElement.prototype.scrollIntoView=function(){};
  function installModule(){w.BritesConciergeExpression={...expressionApi,create(options){controller=expressionApi.create({...options,now:()=>clock});return controller;}};}
  if(moduleAvailable)installModule();
  w.BritesConciergeAvatar={create:()=>({setExpression:value=>calls.expressions.push(value?{...value}:null),setEmotion:value=>calls.emotions.push(value),setLevel:value=>calls.levels.push(value),setState:value=>calls.states.push(value),setPaused(){},setVisible(){},setFloating(){},triggerGreeting(){},cancelPerformance(){},perform:value=>{calls.performances.push({...value});return true;},retry(){},destroy(){}})};
  let config;const client={state:'listening',outputMeterState:'ready',playbackBlocked:false,start:async()=>{calls.starts++;return true;},stop:async()=>{calls.stops++;},interrupt(){calls.interrupts++;},dispose:async()=>{}};
  w.BritesConciergeVoice={create:value=>{config=value;return client;}};
  w.fetch=async(url,init={})=>{const body=init.body?JSON.parse(init.body):{};calls.requests.push({url:String(url),body});return {ok:true,json:async()=>String(url).includes('/api/growth/product?')?{live:true,product}:body.message?{live:true,reply:'Current piece checked.',preferences:{},products:[product],meanings:[]}:{enabled:true}};};
  w.eval(actions);w.eval(widget);const root=d.querySelector('brites-concierge').shadowRoot,button=label=>[...root.querySelectorAll('button')].find(value=>value.textContent.trim()===label||value.getAttribute('aria-label')===label);
  async function start(){w.BritesConcierge.open();await settle();button('Talk to me').click();await settle();root.querySelector('script[src$="brites-concierge-voice.js"]')?.dispatchEvent(new w.Event('load'));await settle();}
  function advance(ms,output=null){const end=clock+ms;while(clock<end){clock=Math.min(end,clock+50);for(const [id,value] of [...timers])if(value.at<=clock){timers.delete(id);value.fn();}if(output!==null)config.onLevel({input:0,output});}}
  function state(value){client.state=value;config.onState(value);}
  function speech(itemId='input-1',turnVersion=1){config.onSpeechStarted({itemId,turnVersion,reason:'speech'});state('listening');return {inputItemId:itemId,turnVersion,currentTurn:true};}
  function transcript(text,turn,{responseId='reply-1',itemId='output-1',final=true}={}){config.onTranscript({role:'assistant',...turn,responseId,itemId,contentIndex:0,outputIndex:0,text,final});}
  function userTranscript(text,turn){config.onTranscript({role:'user',itemId:turn.inputItemId,turnVersion:turn.turnVersion,currentTurn:true,text,final:true});}
  function playback(turn,{playing=true,cleared=false,responseId='reply-1',itemId='output-1'}={}){config.onPlaybackState({...turn,responseId,itemId,playing,cleared});}
  async function loadModule(){installModule();const loader=root.querySelector('script[src$="brites-concierge-expression.js"]');assert.ok(loader,'there is exactly one pending optional expression asset');loader.dispatchEvent(new w.Event('load'));await settle();assert.ok(controller,'script initialized the real expression controller');}
  t.after(()=>{try{w.BritesConcierge.close();}catch{}controller?.destroy();timers.clear();w.close();});
  return {w,d,root,errors,calls,client,button,start,advance,state,speech,transcript,userTranscript,playback,loadModule,get config(){return config;},get controller(){return controller;},saved:()=>JSON.parse(w.sessionStorage.getItem('brites-concierge-v1')),hide(value){hidden=value;d.dispatchEvent(new w.Event('visibilitychange'));}};
}

function beginReply(h,text='Congratulations on your graduation. The pendant has a polished silver surface. Which design do you prefer?'){
  const turn=h.speech();h.state('thinking');h.transcript(text,turn);return turn;
}

test('late successful expression loading joins the current measured speech offset without blocking voice',async t=>{
  const ready=fixture(t),late=fixture(t,{moduleAvailable:false});await ready.start();await late.start();
  const turns=[beginReply(ready),beginReply(late)];
  for(const h of [ready,late])h.advance(1200); // Generation time is not speech time.
  for(const [index,h] of [ready,late].entries()){h.playback(turns[index]);h.state('speaking');h.config.onOutputMeterState('ready');h.advance(1000,.45);}
  const before=ready.controller.snapshot().clockMs;assert.ok(before>=900&&before<=1150);assert.equal(late.calls.starts,1);
  await late.loadModule();const hydrated=late.controller.snapshot();
  assert.equal(hydrated.active,true);assert.equal(hydrated.playing,true);assert.equal(hydrated.turnId,'input-1');assert.equal(hydrated.responseId,'reply-1');assert.equal(hydrated.timing,'estimated-audio-activity');
  assert.ok(Math.abs(hydrated.clockMs-before)<=150,'late load keeps the observed speech offset, not generation time or phrase zero');
  const start=hydrated.clockMs;late.advance(900,.45);assert.ok(late.controller.snapshot().clockMs>=start+750);assert.ok(late.calls.expressions.some(value=>value?.kind==='celebrate'));assert.equal(late.calls.stops,0);assert.equal(late.errors.length,0);
});

test('late loading cannot infer speech progress from generated text, native buffer start or microphone energy',async t=>{
  const h=fixture(t,{moduleAvailable:false});await h.start();const turn=beginReply(h);h.playback(turn);h.state('speaking');
  for(let i=0;i<20;i++){h.config.onLevel({input:.8,output:0});h.advance(50);}
  await h.loadModule();assert.equal(h.controller.snapshot().clockMs,0);assert.equal(h.controller.snapshot().timing,'estimated-audio-activity');assert.equal(h.calls.starts,1);assert.equal(h.calls.levels.at(-1),0);
  h.advance(600,.4);assert.ok(h.controller.snapshot().clockMs>=450);assert.ok(h.calls.expressions.some(Boolean));
});

for(const boundary of ['interrupt','End voice','pause','hidden'])test('late expression asset after '+boundary+' cannot revive old speaking cues',async t=>{
  const h=fixture(t,{moduleAvailable:false});await h.start();const turn=beginReply(h);h.playback(turn);h.state('speaking');h.advance(800,.4);
  if(boundary==='interrupt')h.button('Let me speak').click();else if(boundary==='End voice')h.button('End voice').click();else if(boundary==='pause')h.button('Pause animation').click();else h.hide(true);
  await h.loadModule();assert.equal(h.controller.snapshot().active,false);assert.equal(h.controller.snapshot().cue,null);assert.equal(h.controller.snapshot().phraseCount,0);
  if(boundary==='pause')h.button('Resume animation').click();
  h.transcript('Congratulations! Wonderful news!',turn);h.playback(turn);h.advance(700,.9);
  assert.equal(h.calls.expressions.filter(Boolean).length,0);assert.equal(h.controller.snapshot().active,false);assert.equal(h.calls.starts,1);
});

test('late loading follows only a new current turn and rejects prior-turn transcription and playback',async t=>{
  const h=fixture(t,{moduleAvailable:false});await h.start();const old=beginReply(h);h.playback(old);h.state('speaking');h.advance(700,.5);
  const fresh=h.speech('input-2',2);h.config.onListeningTranscript({role:'user',itemId:fresh.inputItemId,turnVersion:2,currentTurn:true,text:'My sister died last week.',final:true});
  h.transcript('Congratulations on the anniversary!',old);h.playback(old);
  await h.loadModule();h.advance(200);
  const current=h.controller.snapshot();assert.equal(current.turnId,'input-2');assert.equal(current.context,'support');assert.equal(current.playing,false);assert.equal(current.responseId,'');assert.equal(current.phraseCount,0);assert.ok(h.calls.expressions.filter(Boolean).every(value=>value.kind==='support'));assert.equal(h.calls.starts,1);
});

test('late load after output stopped never replays the completed reply on fresh level samples',async t=>{
  const h=fixture(t,{moduleAvailable:false});await h.start();const turn=beginReply(h);h.playback(turn);h.state('speaking');h.advance(500,.5);h.playback(turn,{playing:false});h.state('listening');
  await h.loadModule();const before=h.calls.expressions.filter(Boolean).length;h.advance(1000,.7);
  assert.equal(h.controller.snapshot().playing,false);assert.equal(h.controller.snapshot().clockMs,0);assert.equal(h.calls.expressions.filter(Boolean).slice(before).some(value=>value.kind==='celebrate'),false);
});

for(const moduleAvailable of [true,false])for(const text of [
  'Not for a memorial; this is for a birthday.',
  'No one died; this is an anniversary gift.',
  'This is no longer a memorial; I want a birthday gift.'
])test('actual final user transcript clears quiet guard for '+JSON.stringify(text)+(moduleAvailable?' with planner':' before planner loads'),async t=>{
  const h=fixture(t,{moduleAvailable});await h.start();const first=h.speech();h.userTranscript('My sister died last week.',first);assert.equal(h.calls.emotions.at(-1),'calm');
  const fresh=h.speech('input-2',2);h.userTranscript(text,fresh);assert.equal(h.calls.emotions.at(-1),'celebrate');
  h.state('thinking');h.config.onAvatarPerformance({mood:'celebrate',gesture:'confirm',intensity:.45,durationMs:1000},fresh);h.playback(fresh,{responseId:'reply-2',itemId:'output-2'});h.state('speaking');assert.equal(h.calls.performances.at(-1)?.mood,'celebrate');assert.equal(h.calls.performances.at(-1)?.gesture,'confirm');
  if(!moduleAvailable)await h.loadModule();assert.equal(h.calls.starts,1);assert.equal(h.errors.length,0);
});

test('negating a memorial label does not suppress explicitly stated bereavement in the actual widget',async t=>{
  const h=fixture(t);await h.start();const turn=h.speech();h.userTranscript('Not a memorial gift, but my sister died last week.',turn);assert.equal(h.calls.emotions.at(-1),'calm');
  h.state('thinking');h.config.onAvatarPerformance({mood:'celebrate',gesture:'confirm',intensity:.8,durationMs:1000},turn);h.playback(turn);h.state('speaking');assert.equal(h.calls.performances.at(-1)?.mood,'calm');assert.equal(h.calls.performances.at(-1)?.gesture,'reassure');assert.ok(h.calls.performances.at(-1)?.intensity<=.35);
});

for(const moduleAvailable of [true,false])test('a same-selection bereavement follow-up retains quiet context until explicit release'+(moduleAvailable?' with planner':' before planner loads'),async t=>{
  const h=fixture(t,{moduleAvailable});await h.start();const first=h.speech();h.userTranscript('My sister died last week.',first);assert.equal(h.calls.emotions.at(-1),'calm');
  const second=h.speech('input-2',2);h.userTranscript('Would a birthday flower be suitable?',second);assert.equal(h.calls.emotions.at(-1),'calm','a milestone question does not announce a different conversation');
  h.state('thinking');h.config.onAvatarPerformance({mood:'celebrate',gesture:'confirm',intensity:.8,durationMs:1000},second);h.playback(second,{responseId:'reply-2',itemId:'output-2'});h.state('speaking');assert.equal(h.calls.performances.at(-1)?.mood,'calm');assert.equal(h.calls.performances.at(-1)?.gesture,'reassure');
  const third=h.speech('input-3',3);h.userTranscript('Another gift: this is for a wedding anniversary.',third);assert.equal(h.calls.emotions.at(-1),'celebrate','an explicit independent gift can release the quiet context');
  if(!moduleAvailable)await h.loadModule();assert.equal(h.calls.starts,1);
});

for(const moduleAvailable of [true,false])for(const text of [
  'I am not confused; please show the dimensions.',
  'I am not frustrated; I just want the dimensions.',
  'I am not annoyed; I am comparing the styles.',
  'I am not overwhelmed; I am considering two designs.',
  'The clasp is not broken. Which length would you choose?',
  'That is not wrong; I just prefer the other style.'
])test('locally negated difficulty does not create a repair face for '+JSON.stringify(text)+(moduleAvailable?' with planner':' before planner loads'),async t=>{
  const h=fixture(t,{moduleAvailable});await h.start();const turn=h.speech();h.userTranscript(text,turn);
  assert.equal(['calm','reassuring'].includes(h.calls.emotions.at(-1)),false,'explicitly denied difficulty is not an observed repair context');
  h.state('thinking');h.config.onAvatarPerformance({mood:'curious',gesture:'explain',intensity:.55,durationMs:1000},turn);h.playback(turn);h.state('speaking');assert.equal(h.calls.performances.at(-1)?.mood,'curious');assert.equal(h.calls.performances.at(-1)?.gesture,'explain');
  if(!moduleAvailable)await h.loadModule();assert.equal(h.calls.starts,1);assert.equal(h.errors.length,0);
});

for(const moduleAvailable of [true,false])for(const text of [
  'I am not confused, but the clasp is broken.',
  'The clasp is not broken, but the chain is not working.'
])test('negating one difficulty preserves an actual later failure in '+JSON.stringify(text)+(moduleAvailable?' with planner':' before planner loads'),async t=>{
  const h=fixture(t,{moduleAvailable});await h.start();const turn=h.speech();h.userTranscript(text,turn);assert.equal(h.calls.emotions.at(-1),'reassuring');
  h.state('thinking');h.config.onAvatarPerformance({mood:'celebrate',gesture:'confirm',intensity:.8,durationMs:1000},turn);h.playback(turn);h.state('speaking');assert.equal(h.calls.performances.at(-1)?.mood,'reassuring');assert.equal(h.calls.performances.at(-1)?.gesture,'reassure');assert.ok(h.calls.performances.at(-1)?.intensity<=.45);
  if(!moduleAvailable)await h.loadModule();assert.equal(h.calls.starts,1);assert.equal(h.errors.length,0);
});
