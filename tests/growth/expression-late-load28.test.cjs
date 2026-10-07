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
  const timers=new Map(),calls={expressions:[],expressionLevels:[],expressionPlaybacks:[],emotions:[],levels:[],states:[],performances:[],requests:[],starts:0,stops:0,interrupts:0};
  w.setTimeout=(fn,ms)=>{const id=++serial;timers.set(id,{fn,at:clock+Number(ms||0)});return id;};w.clearTimeout=id=>timers.delete(id);
  w.HTMLElement.prototype.scrollIntoView=function(){};
  function installModule(){w.BritesConciergeExpression={...expressionApi,create(options){controller=expressionApi.create({...options,now:()=>clock});const level=controller.level,playback=controller.playback;controller.level=value=>{calls.expressionLevels.push({...value});return level(value);};controller.playback=value=>{calls.expressionPlaybacks.push({...value});return playback(value);};return controller;}};}
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

// The clock below is local media time, independent of generation and host wall
// time. It is delivered through the real widget callback into the production
// planner; the fixture records boundary arguments without replacing behavior.
function outputSample(h,turn,outputTimeMs,{wallMs=1000,output=.4,missingTime=false,...metadata}={}){
  h.advance(wallMs);
  const value={input:0,output,outputClock:'local-media-currentTime',responseId:'reply-1',...turn,...metadata};
  if(!missingTime)value.outputTimeMs=outputTimeMs;
  h.config.onLevel(value);
  return value;
}
function playingReply(h){const turn=beginReply(h);h.playback(turn);h.state('speaking');h.config.onOutputMeterState('ready');return turn;}

test('widget forwards response-qualified local media evidence without converting it to a wall frame',async t=>{
  const h=fixture(t);await h.start();const turn=playingReply(h);
  const first=outputSample(h,turn,25000,{wallMs:1000});
  assert.equal(h.controller.snapshot().clockMs,0,'the first measured sample anchors even at a nonzero media position');
  const second=outputSample(h,turn,25150,{wallMs:1200});
  assert.equal(h.controller.snapshot().clockMs,150,'sparse widget callbacks preserve the measured interval rather than the longer host delay');
  for(const [actual,expected] of h.calls.expressionLevels.slice(-2).map((value,index)=>[value,[first,second][index]])){
    for(const key of ['input','output','outputTimeMs','outputClock','responseId','inputItemId','turnVersion','currentTurn'])assert.equal(actual[key],expected[key],key+' survives the widget boundary');
  }
  assert.equal(h.calls.starts,1);assert.equal(h.calls.stops,0);
});

test('late planner hydration preserves measured speech offset and seeds the last admitted media observation',async t=>{
  const h=fixture(t,{moduleAvailable:false});await h.start();const turn=beginReply(h);
  h.advance(1200); // Generated text existed before native output began.
  h.playback(turn);h.state('speaking');h.config.onOutputMeterState('ready');
  outputSample(h,turn,25000,{wallMs:1000});
  outputSample(h,turn,25900,{wallMs:1000});
  await h.loadModule();
  assert.equal(h.controller.snapshot().clockMs,900,'pending hydration uses adjacent positive media observations, excluding generation delay');
  assert.equal(h.calls.expressionPlaybacks.at(-1).startOffsetMs,900);
  assert.equal(h.calls.expressionLevels.at(-1).outputTimeMs,25900,'hydrate the last admitted sample after playback sets the offset');
  assert.equal(h.calls.expressionLevels.at(-1).responseId,'reply-1');
  outputSample(h,turn,26550,{wallMs:650});
  assert.equal(h.controller.snapshot().clockMs,1550,'the first post-load sample continues the seeded interval instead of losing 650ms');
  assert.equal(h.calls.starts,1);assert.equal(h.calls.stops,0);
});

test('already loaded and late planners agree after a sparse response-qualified measured series',async t=>{
  const ready=fixture(t),late=fixture(t,{moduleAvailable:false});await ready.start();await late.start();
  const turns=[playingReply(ready),playingReply(late)];
  for(const [index,h] of [ready,late].entries())for(const timestamp of [50000,50500,51500,51650])outputSample(h,turns[index],timestamp,{wallMs:750});
  await late.loadModule();
  assert.equal(ready.controller.snapshot().clockMs,1650);assert.equal(late.controller.snapshot().clockMs,1650);
  for(const [index,h] of [ready,late].entries())outputSample(h,turns[index],52050,{wallMs:1000});
  assert.equal(ready.controller.snapshot().clockMs,2050);assert.equal(late.controller.snapshot().clockMs,2050);
});

test('pending silence breaks an adjacent positive interval without counting the silent gap as speech',async t=>{
  const h=fixture(t,{moduleAvailable:false});await h.start();const turn=playingReply(h);
  outputSample(h,turn,1000,{wallMs:100});outputSample(h,turn,1250,{wallMs:250});
  outputSample(h,turn,1750,{wallMs:500,output:0});
  outputSample(h,turn,2250,{wallMs:500});outputSample(h,turn,2500,{wallMs:250});
  await h.loadModule();assert.equal(h.controller.snapshot().clockMs,500,'only the two separate positive intervals belong to observed speech');
  outputSample(h,turn,2650,{wallMs:150});assert.equal(h.controller.snapshot().clockMs,650);
});

for(const [gap,expected] of [[2000,2000],[2001,0]])test('late-load pending clock '+(gap===2000?'admits':'rejects')+' an adjacent '+gap+'ms media interval',async t=>{
  const h=fixture(t,{moduleAvailable:false});await h.start();const turn=playingReply(h);
  outputSample(h,turn,10000,{wallMs:100});outputSample(h,turn,10000+gap,{wallMs:100});
  await h.loadModule();assert.equal(h.controller.snapshot().clockMs,expected);
  outputSample(h,turn,10150+gap,{wallMs:150});assert.equal(h.controller.snapshot().clockMs,expected+150,'a discontinuity re-anchors, then valid adjacent playback can continue');
});

for(const moduleAvailable of [true,false])for(const invalid of ['missing',NaN,Infinity,-1,'1500'])test('widget cannot fabricate wall progress after measured playback loses its '+String(invalid)+' timestamp'+(moduleAvailable?' with planner':' before planner loads'),async t=>{
  const h=fixture(t,{moduleAvailable});await h.start();const turn=playingReply(h);
  outputSample(h,turn,1000,{wallMs:100});outputSample(h,turn,1150,{wallMs:150});
  outputSample(h,turn,invalid,{wallMs:1000,missingTime:invalid==='missing'});
  h.advance(400);
  if(!moduleAvailable)await h.loadModule();
  assert.equal(h.controller.snapshot().clockMs,150,'clock failure preserves prior measured progress, even while positive RMS remains available');
  outputSample(h,turn,2500,{wallMs:250});assert.equal(h.controller.snapshot().clockMs,150,'the first valid returned timestamp re-anchors after missing or invalid evidence');
  outputSample(h,turn,2700,{wallMs:200});assert.equal(h.controller.snapshot().clockMs,350);
});

for(const moduleAvailable of [true,false])for(const stale of [
  {name:'another response',responseId:'reply-old'},
  {name:'another input',inputItemId:'input-old'},
  {name:'another turn version',turnVersion:0},
  {name:'explicit old-turn classification',currentTurn:false}
])test('widget rejects '+stale.name+' audio before it changes '+(moduleAvailable?'the active':'the pending')+' expression clock',async t=>{
  const h=fixture(t,{moduleAvailable});await h.start();const turn=playingReply(h);
  outputSample(h,turn,1000,{wallMs:100});outputSample(h,turn,1200,{wallMs:200});
  const before=h.calls.expressionLevels.length,mouthBefore=h.calls.levels.length;
  const {name,...metadata}=stale;outputSample(h,turn,1300,{wallMs:100,output:.9,...metadata});
  if(moduleAvailable){assert.equal(h.controller.snapshot().clockMs,200);assert.equal(h.calls.expressionLevels.length,before,'stale level is rejected at the widget boundary');}
  assert.equal(h.calls.levels.length,mouthBefore,'stale RMS does not animate the actual avatar mouth sink');
  outputSample(h,turn,1350,{wallMs:50});
  if(!moduleAvailable)await h.loadModule();
  assert.equal(h.controller.snapshot().clockMs,350,'stale media never replaces the last admitted timestamp or gains pending activity');
  outputSample(h,turn,1500,{wallMs:150});assert.equal(h.controller.snapshot().clockMs,500);
});

test('a legacy missing-clock pending series retains its bounded fallback only until measured evidence arrives',async t=>{
  const h=fixture(t,{moduleAvailable:false});await h.start();const turn=playingReply(h);
  outputSample(h,turn,undefined,{wallMs:2000,missingTime:true});
  outputSample(h,turn,undefined,{wallMs:1500,missingTime:true});
  await h.loadModule();assert.equal(h.controller.snapshot().clockMs,200,'each legacy pending sample admits at most 100ms despite sparse host callbacks');
  h.advance(100);const beforeMeasured=h.controller.snapshot().clockMs;
  assert.ok(beforeMeasured>=200&&beforeMeasured<=300,'before any measured clock, the existing fallback may follow its just-observed output for at most this 100ms wall interval');
  outputSample(h,turn,50000,{wallMs:0});assert.equal(h.controller.snapshot().clockMs,beforeMeasured,'the first measured sample does not add its absolute media position');
  outputSample(h,turn,50200,{wallMs:200});assert.equal(h.controller.snapshot().clockMs,beforeMeasured+200);
  outputSample(h,turn,undefined,{wallMs:1000,missingTime:true});h.advance(300);
  assert.equal(h.controller.snapshot().clockMs,beforeMeasured+200,'a measured response never reverts to fabricated wall progress when its optional clock disappears');
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
