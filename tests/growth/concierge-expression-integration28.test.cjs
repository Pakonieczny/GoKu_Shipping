'use strict';
// Real widget plus real expression controller, synthetic voice callbacks and
// avatar sink. Visual rendering, microphones and audio alignment need browser QA.
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs');
const {JSDOM,VirtualConsole}=require('jsdom');
const expressionApi=require('../../brites-concierge-expression.js');
const widget=fs.readFileSync(require.resolve('../../brites-concierge.js'),'utf8');
const actions=fs.readFileSync(require.resolve('../../brites-concierge-voice-actions.js'),'utf8');
const tick=()=>new Promise(resolve=>setImmediate(resolve));
async function settle(){await tick();await tick();}
const product={id:'gid://shopify/Product/1',handle:'compass-necklace',url:'https://britesjewelry.com/products/compass-necklace',title:'Compass Necklace',currency:'USD',type:'Necklace',variantsComplete:true,minPrice:54,suggestedVariantId:'gid://shopify/ProductVariant/101',variants:[{id:'gid://shopify/ProductVariant/101',numericId:'101',title:'Sterling Silver / 18 inch',price:54,available:true,options:[{name:'Metal',value:'Sterling Silver'},{name:'Length',value:'18 inch'}]}]};
function fixture(t,{moduleAvailable=true}={}){
  const errors=[],vc=new VirtualConsole();vc.on('jsdomError',value=>errors.push(value));
  const dom=new JSDOM('<!doctype html><body></body>',{url:'https://preview.example/concierge-sandbox.html',pretendToBeVisual:true,runScripts:'outside-only',virtualConsole:vc}),w=dom.window,d=w.document;
  const script=d.createElement('script');script.src='/brites-concierge.js';script.dataset.sandbox='true';Object.defineProperty(d,'currentScript',{get:()=>script});
  let hidden=false,clock=1791340000000,serial=0,controller=null;Object.defineProperty(d,'hidden',{get:()=>hidden});w.Date.now=()=>clock;
  const timers=new Map(),calls={expressions:[],emotions:[],levels:[],states:[],performances:[],requests:[],starts:0,stops:0,interrupts:0};
  w.setTimeout=(fn,ms)=>{const id=++serial;timers.set(id,{fn,at:clock+Number(ms||0)});return id;};w.clearTimeout=id=>timers.delete(id);
  w.HTMLElement.prototype.scrollIntoView=function(){};
  if(moduleAvailable)w.BritesConciergeExpression={...expressionApi,create(options){controller=expressionApi.create({...options,now:()=>clock});return controller;}};
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
  function playback(turn,{playing=true,cleared=false,responseId='reply-1',itemId='output-1'}={}){config.onPlaybackState({...turn,responseId,itemId,playing,cleared});}
  t.after(()=>{try{w.BritesConcierge.close();}catch{}controller?.destroy();timers.clear();w.close();});
  return {w,d,root,errors,calls,client,button,start,advance,state,speech,transcript,playback,get config(){return config;},get controller(){return controller;},saved:()=>JSON.parse(w.sessionStorage.getItem('brites-concierge-v1')),hide(value){hidden=value;d.dispatchEvent(new w.Event('visibilitychange'));}};
}

test('generation and final caption do not jump the face to the final clause before native output',async t=>{
  const h=fixture(t);await h.start();assert.equal(h.calls.starts,1);const turn=h.speech();h.state('thinking');const before=h.calls.emotions.length;
  h.transcript('Congratulations on your graduation. This piece may be a personal reminder. Which design do you prefer?',turn);h.advance(2000);
  assert.equal(h.calls.emotions.length,before);assert.equal(h.controller.snapshot().clockMs,0);assert.equal(h.calls.expressions.filter(Boolean).at(-1)?.kind==='inquiry',false);
  assert.equal(h.root.querySelector('.caption-text').textContent,'Congratulations on your graduation. This piece may be a personal reminder. Which design do you prefer?');assert.equal(h.errors.length,0);
});

test('a single real-controller reply changes phrase intent with observed output, then rests during silence',async t=>{
  const h=fixture(t);await h.start();const turn=h.speech();h.state('thinking');h.transcript('Congratulations on your graduation. The silver pendant has a simple polished shape. Which design do you prefer?',turn);h.playback(turn);h.state('speaking');h.config.onOutputMeterState('ready');
  h.advance(9000,.4);const kinds=new Set(h.calls.expressions.filter(Boolean).map(value=>value.kind));assert.ok(kinds.has('celebrate'));assert.ok(kinds.has('explain'));assert.ok(kinds.has('inquiry'));assert.ok(h.controller.snapshot().transitions>=3);assert.equal(h.calls.emotions.includes('curious'),false,'the old coarse final-transcript tone is not used');
  const clock=h.controller.snapshot().clockMs;h.config.onLevel({input:1,output:0});h.advance(1000);assert.ok(h.controller.snapshot().clockMs-clock<=250);assert.equal(h.calls.levels.at(-1),0,'microphone activity cannot animate a speaking mouth');
});

test('partial listening text can change presentation but cannot authorize options or save shopper history',async t=>{
  const h=fixture(t);await h.start();await h.config.onTool({message:'Show a compass necklace'},{name:'find_jewellery'});assert.equal(h.root.querySelectorAll('.card').length,1);const turn=h.speech();
  h.config.onListeningTranscript({delta:'Show options for the Compass Necklace.',final:false,itemId:turn.inputItemId,turnVersion:turn.turnVersion,currentTurn:true});h.advance(100);
  const result=await h.config.onTool({handle:product.handle,action:'options'},{name:'prepare_jewellery_action',...turn});assert.match(result.error,/tell me which displayed piece/i);assert.equal(h.root.querySelector('select'),null);assert.equal(h.w.sessionStorage.getItem('brites-sandbox-cart'),null);assert.equal(h.saved().history.some(value=>value.content==='Show options for the Compass Necklace.'),false);
  assert.equal(h.calls.requests.some(value=>value.body.action==='add'||value.body.action==='checkout'),false);assert.equal(h.calls.starts,1);
});

for(const boundary of ['interrupt','End voice','pause','new input'])test(boundary+' cancels current expressions and obsolete callbacks cannot revive them',async t=>{
  const h=fixture(t);await h.start();const turn=h.speech();h.state('thinking');h.transcript('Congratulations on your graduation. Which piece is right for you?',turn);h.playback(turn);h.state('speaking');h.advance(800,.5);
  if(boundary==='interrupt')h.button('Let me speak').click();else if(boundary==='End voice')h.button('End voice').click();else if(boundary==='pause')h.button('Pause animation').click();else h.speech('input-2',2);
  const count=h.calls.expressions.filter(Boolean).length;h.transcript('Congratulations! Wonderful news!',turn);h.playback(turn);h.config.onListeningTranscript({delta:'Congratulations!',final:false,itemId:turn.inputItemId,turnVersion:turn.turnVersion,currentTurn:true});h.advance(1000,.9);
  if(boundary==='new input'){assert.ok(h.calls.expressions.filter(Boolean).slice(count).every(value=>value.kind==='attentive'));assert.equal(h.controller.snapshot().turnId,'input-2');}else assert.equal(h.calls.expressions.filter(Boolean).length,count);assert.equal(h.calls.starts,1);if(boundary==='pause')assert.equal(h.calls.stops,0);
});

test('a model performance queued while thinking starts on real adapter callback order',async t=>{
  const h=fixture(t);await h.start();const turn=h.speech();h.state('thinking');const cue={mood:'curious',gesture:'explain',intensity:.35,durationMs:1250};h.config.onAvatarPerformance(cue,turn);assert.equal(h.calls.performances.length,0);
  h.playback(turn);assert.equal(h.calls.performances.length,0,'native buffer notification precedes onState speaking');h.state('speaking');assert.equal(h.calls.performances.length,1);assert.equal(h.calls.performances[0].gesture,'explain');
});

test('a queued model performance is discarded by a new input and never borrowed by its reply',async t=>{
  const h=fixture(t);await h.start();const turn=h.speech();h.state('thinking');h.config.onAvatarPerformance({mood:'celebrate',gesture:'confirm',intensity:.4,durationMs:1100},turn);const fresh=h.speech('input-2',2);h.state('thinking');h.transcript('I will check that for you.',fresh,{responseId:'reply-2',itemId:'output-2'});h.playback(fresh,{responseId:'reply-2',itemId:'output-2'});h.state('speaking');h.advance(800,.4);assert.equal(h.calls.performances.length,0);
});

test('an optional expression asset that never loads does not delay or disable voice startup',async t=>{
  const h=fixture(t,{moduleAvailable:false});await h.start();assert.equal(h.calls.starts,1,'voice starts without awaiting the optional seven-second expression load');assert.equal(h.button('End voice').getAttribute('aria-pressed'),'true');
  h.root.querySelector('script[src$="brites-concierge-expression.js"]')?.dispatchEvent(new h.w.Event('error'));await settle();h.config.onState('listening');assert.equal(h.calls.stops,0);assert.equal(h.calls.starts,1);assert.equal(h.root.querySelector('.voice-state').textContent,'Listening to you');
});
