'use strict';
// Real widget + expression planner, with synthetic native-buffer callbacks and
// an avatar boundary sink. This does not test microphones, audible media, GPU,
// phoneme alignment, or human interpretation of the face.
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs');
const {JSDOM,VirtualConsole}=require('jsdom');
const Expression=require('../../brites-concierge-expression.js');
const source=fs.readFileSync(require.resolve('../../brites-concierge.js'),'utf8');
const actions=fs.readFileSync(require.resolve('../../brites-concierge-voice-actions.js'),'utf8');
const settle=async()=>{await new Promise(resolve=>setImmediate(resolve));await new Promise(resolve=>setImmediate(resolve));};
const product={id:'gid://shopify/Product/1',handle:'compass-necklace',url:'https://britesjewelry.com/products/compass-necklace',title:'Compass Necklace',type:'Necklace',currency:'USD',minPrice:54,variantsComplete:true,suggestedVariantId:'gid://shopify/ProductVariant/101',variants:[{id:'gid://shopify/ProductVariant/101',numericId:'101',title:'Sterling Silver / 18 inch',price:54,available:true,options:[{name:'Metal',value:'Sterling Silver'},{name:'Length',value:'18 inch'}]},{id:'gid://shopify/ProductVariant/102',numericId:'102',title:'Sterling Silver / 20 inch',price:56,available:true,options:[{name:'Metal',value:'Sterling Silver'},{name:'Length',value:'20 inch'}]}]};
const ZERO={amplitude:0,bands:[0,0,0,0,0,0],brightness:0,valid:false};
function signal(amplitude=.4){return {amplitude,bands:[.08,.16,.35,.22,.1,.04],brightness:.27,valid:true};}

function fixture(t,{avatarAvailable=true,legacyRig=false,nativeGetter=true}={}){
  const errors=[],vc=new VirtualConsole();vc.on('jsdomError',error=>errors.push(error));
  const dom=new JSDOM('<!doctype html><body></body>',{url:'https://preview.example/concierge-sandbox.html',pretendToBeVisual:true,runScripts:'outside-only',virtualConsole:vc});
  const w=dom.window,d=w.document;let hidden=false,clock=1791350000000,serial=0,controller=null,sink=null,config=null;
  Object.defineProperty(d,'hidden',{get:()=>hidden});w.Date.now=()=>clock;const motionListeners=[];const motion={matches:false,addEventListener:(name,listener)=>motionListeners.push(listener)};w.matchMedia=()=>motion;
  const timers=new Map(),calls={signals:[],levels:[],states:[],emotions:[],creations:0,starts:0,stops:0,disposes:0,requests:[],configs:[]};
  w.setTimeout=(fn,ms)=>{const id=++serial;timers.set(id,{fn,at:clock+Number(ms||0)});return id;};w.clearTimeout=id=>timers.delete(id);w.HTMLElement.prototype.scrollIntoView=function(){};
  const script=d.createElement('script');script.src='/brites-concierge.js';script.dataset.sandbox='true';Object.defineProperty(d,'currentScript',{get:()=>script});
  w.sessionStorage.setItem('brites-concierge-v1',JSON.stringify({updatedAt:clock,open:false,history:[],preferences:{},products:[product],meanings:[],policyLinks:[],productHandles:[product.handle],selectedVariants:{},uncertainVariants:[]}));
  w.BritesConciergeExpression={...Expression,create(options){controller=Expression.create({...options,now:()=>clock});return controller;}};
  function installAvatar(){w.BritesConciergeAvatar={create(options){calls.creations++;sink={state:options.initialState,level:0,currentSignal:null,paused:false,visible:options.visible};const api={setState(value){calls.states.push(value);if(value!==sink.state){sink.state=value;if(value!=='speaking'){sink.level=0;sink.currentSignal=null;}}},setLevel(value){calls.levels.push(value);sink.level=value;sink.currentSignal=null;},setExpression(){},setEmotion(value){calls.emotions.push(value);},setPaused(value){sink.paused=value;if(value){sink.level=0;sink.currentSignal=null;}},setVisible(value){sink.visible=value;if(!value){sink.level=0;sink.currentSignal=null;}},setFloating(){},triggerGreeting(){},cancelPerformance(){},clearFocus(){},clearProduct(){},focusProduct(){},showProduct(){return true;},cue(){},perform(){return true;},retry(){},destroy(){}};if(!legacyRig)api.setSpeechSignal=value=>{calls.signals.push(value?JSON.parse(JSON.stringify(value)):null);sink.currentSignal=value?JSON.parse(JSON.stringify(value)):null;sink.level=value?.valid===true?value.amplitude:0;return true;};return api;}};}
  if(avatarAvailable)installAvatar();
  const clients=[];
  w.BritesConciergeVoice={create(value){config=value;calls.configs.push(value);const client={state:'listening',outputMeterState:'ready',playbackBlocked:false,native:null,start:async()=>{calls.starts++;client.native=null;client.state='listening';return true;},stop:async()=>{calls.stops++;client.native=null;client.state='idle';},dispose:async()=>{calls.disposes++;client.native=null;client.state='idle';},interrupt(){client.native=null;client.state='listening';value.onState('listening');},updateContext(){}};if(nativeGetter)Object.defineProperty(client,'currentOutput',{get:()=>client.native?{...client.native}:null});clients.push(client);return client;}};
  w.fetch=async(raw,init={})=>{const body=init.body?JSON.parse(init.body):{};calls.requests.push({url:String(raw),body});return {ok:true,json:async()=>String(raw).includes('/api/growth/product?')?{live:true,product}:body.message?{live:true,reply:'The live piece was checked.',preferences:{},products:[product],meanings:[]}:{enabled:true}};};
  w.eval(actions);w.eval(source);const root=d.querySelector('brites-concierge').shadowRoot;
  const button=label=>[...root.querySelectorAll('button')].find(node=>node.textContent.trim()===label||node.getAttribute('aria-label')===label);
  const h={w,d,root,calls,errors,button,get config(){return config;},get client(){return clients.at(-1);},get sink(){return sink;},get controller(){return controller;},async start(){w.BritesConcierge.open();await settle();button('Talk to me').click();await settle();root.querySelector('script[src$="brites-concierge-voice.js"]')?.dispatchEvent(new w.Event('load'));await settle();},advance(ms){const end=clock+ms;while(clock<end){clock=Math.min(end,clock+50);for(const [id,value] of [...timers])if(value.at<=clock){timers.delete(id);value.fn();}}},state(value){h.client.state=value;config.onState(value);},meter(value){h.client.outputMeterState=value;config.onOutputMeterState(value);},speech(itemId='input-1',turnVersion=1,text='Show options for the first one.'){h.client.native=null;config.onSpeechStarted({itemId,turnVersion,reason:'speech'});h.state('listening');config.onTranscript({role:'user',itemId,turnVersion,currentTurn:true,text,final:true});return {inputItemId:itemId,turnVersion,currentTurn:true};},playback(turn,{playing=true,responseId='reply-1',...extra}={}){const value={...turn,responseId,itemId:'output-'+responseId,playing,...extra};h.client.native=playing?{...value,currentTurn:true}:null;config.onPlaybackState(value);return value;},speak(turn,responseId='reply-1'){config.onTranscript({role:'assistant',...turn,responseId,itemId:'output-'+responseId,text:'Here is the compass design. Which length would you prefer?',final:true});h.playback(turn,{responseId});h.state('speaking');h.meter('ready');},sample(turn,amplitude=.4,extra={}){const value={input:0,output:amplitude,outputSignal:signal(amplitude),outputTimeMs:1000,outputClock:'local-media-currentTime',responseId:h.client.native?.responseId||'reply-1',...turn,...extra};config.onLevel(value);return value;},async loadAvatar(){installAvatar();const loader=root.querySelector('script[src$="brites-concierge-avatar.js"]');assert.ok(loader);loader.dispatchEvent(new w.Event('load'));await settle();},hide(value){hidden=value;d.dispatchEvent(new w.Event('visibilitychange'));},reduced(value){motion.matches=value;motionListeners.forEach(listener=>listener({matches:value}));}};
  t.after(()=>{try{w.BritesConcierge.close();}catch{}controller?.destroy();timers.clear();w.close();});return h;
}
async function speakingFixture(t,options){const h=fixture(t,options);await h.start();const turn=h.speech();h.speak(turn);return {h,turn};}

test('current measured six-band signal reaches the full rig without a following amplitude-only erase',async t=>{
  const {h,turn}=await speakingFixture(t);const count=h.calls.levels.length;h.sample(turn,.46);
  assert.deepEqual(h.sink.currentSignal,signal(.46));assert.equal(h.sink.level,.46);assert.equal(h.calls.levels.length,count);assert.equal(h.calls.starts,1);assert.equal(h.errors.length,0);
});

test('real catalogue tool completion, tray expansion, options and variant change retain current native speech',async t=>{
  const {h,turn}=await speakingFixture(t);h.sample(turn,.44);
  const checked=await h.config.onTool({message:'Show a compass necklace.'},{name:'find_jewellery',...turn});assert.equal(checked.verified,true);
  assert.equal(h.root.querySelector('.product-tray').hidden,false);assert.equal(h.sink.state,'speaking');assert.equal(h.sink.level,.44);
  h.button('Choose options').click();assert.ok(h.root.querySelector('.options select'));assert.equal(h.sink.state,'speaking');assert.equal(h.sink.level,.44);
  h.sample(turn,.61,{outputTimeMs:1200});assert.equal(h.sink.level,.61);
  const select=h.root.querySelector('.options select');select.value=product.variants[1].id;select.dispatchEvent(new h.w.Event('change'));assert.equal(h.sink.state,'speaking');assert.equal(h.sink.level,.61);
  h.button('Hide pieces').click();assert.equal(h.sink.level,.61);h.button('Show pieces').click();assert.equal(h.sink.level,.61);assert.equal(h.calls.creations,1);assert.equal(h.calls.starts,1);assert.equal(h.calls.stops,0);
});

test('an exactly authorized voice options tool does not silence a still-playing native reply',async t=>{
  const {h,turn}=await speakingFixture(t);h.sample(turn,.52);
  const result=await h.config.onTool({action:'options',handle:product.handle},{name:'prepare_jewellery_action',...turn});
  assert.equal(result.prepared,true);assert.equal(result.cartChanged,false);assert.ok(h.root.querySelector('.options select'));assert.equal(h.sink.state,'speaking');assert.equal(h.sink.level,.52);
  assert.equal(h.w.sessionStorage.getItem('brites-sandbox-cart'),null);assert.equal(h.calls.starts,1);assert.equal(h.calls.stops,0);
});

test('a tool-generation thinking state cannot override the current native-buffer speaking interval',async t=>{
  const {h,turn}=await speakingFixture(t);h.sample(turn,.43);h.state('thinking');
  assert.equal(h.sink.state,'speaking');assert.equal(h.sink.level,.43);assert.equal(h.root.querySelector('.voice-state').textContent,'Your guide is speaking');
  h.sample(turn,.57,{outputTimeMs:1300});assert.equal(h.sink.level,.57);
  h.playback(turn,{playing:false});h.state('listening');assert.equal(h.sink.level,0);assert.equal(h.sink.state,'listening');
});

test('a newer generated response does not replace the older buffer that is still playing in the same turn',async t=>{
  const {h,turn}=await speakingFixture(t);h.sample(turn,.37);
  h.config.onTranscript({role:'assistant',...turn,responseId:'reply-generated-next',itemId:'output-next',text:'Here are the checked options.',final:false});h.state('thinking');
  h.sample(turn,.54,{responseId:'reply-1',outputTimeMs:1300});assert.equal(h.sink.state,'speaking');assert.equal(h.sink.level,.54);
  const count=h.calls.signals.length;h.sample(turn,.9,{responseId:'reply-generated-next'});assert.equal(h.calls.signals.length,count,'unplayed generated audio cannot become the current face signal');
});

test('semantic planner retirement cannot veto current measured native output',async t=>{
  const {h,turn}=await speakingFixture(t);h.sample(turn,.31);
  assert.equal(h.controller.playback({...turn,responseId:'reply-1',playing:false}),true);
  assert.equal(h.controller.snapshot().playing,false);
  h.sample(turn,.66,{outputTimeMs:1300});assert.equal(h.sink.level,.66);assert.deepEqual(h.sink.currentSignal.bands,signal(.66).bands);assert.equal(h.sink.state,'speaking');
});

for(const stale of [{currentTurn:false},{turnVersion:0},{inputItemId:'old-input'},{responseId:'old-response'}])test('stale identity '+JSON.stringify(stale)+' cannot replace or clear current output',async t=>{
  const {h,turn}=await speakingFixture(t);h.sample(turn,.48);const count=h.calls.signals.length;
  h.sample(turn,.95,stale);assert.equal(h.calls.signals.length,count);assert.equal(h.sink.level,.48);
  h.sample(turn,0,{...stale,outputSignal:ZERO});assert.equal(h.calls.signals.length,count,'stale zero callbacks cannot silence a current buffer');
});

test('a native stop closes the mouth immediately and rejects late levels from that response',async t=>{
  const {h,turn}=await speakingFixture(t);h.sample(turn,.58);h.playback(turn,{playing:false});assert.equal(h.sink.level,0);
  const count=h.calls.signals.length;h.sample(turn,.9);assert.equal(h.calls.signals.length,count);assert.equal(h.sink.level,0);
});

test('a new native reply drops prior frequencies and admits only its current measured spectrum',async t=>{
  const {h,turn}=await speakingFixture(t);h.sample(turn,.58);h.playback(turn,{responseId:'reply-2'});assert.equal(h.sink.level,0);
  const count=h.calls.signals.length;h.sample(turn,.9,{responseId:'reply-1'});assert.equal(h.calls.signals.length,count);
  h.sample(turn,.36,{responseId:'reply-2'});assert.equal(h.sink.level,.36);
});

test('late avatar loading joins only the newest fresh measured signal and owns a copy of its bins',async t=>{
  const {h,turn}=await speakingFixture(t,{avatarAvailable:false});h.sample(turn,.2);const latest=h.sample(turn,.63);latest.outputSignal.bands[0]=1;
  await h.loadAvatar();assert.equal(h.calls.creations,1);assert.equal(h.sink.state,'speaking');assert.equal(h.sink.level,.63);assert.equal(h.sink.currentSignal.bands[0],.08);assert.equal(h.calls.signals.filter(value=>value?.valid).length,1);
});

test('late avatar loading cannot replay an old positive signal after its fresh observation expires',async t=>{
  const {h,turn}=await speakingFixture(t,{avatarAvailable:false});h.sample(turn,.7);h.advance(300);await h.loadAvatar();assert.equal(h.sink.level,0);assert.equal(h.calls.signals.some(value=>value?.amplitude>0),false);
  h.sample(turn,.34,{outputTimeMs:1300});assert.equal(h.sink.level,.34);
});

for(const boundary of ['pause','blocked','unavailable','input','stop','hidden'])test('late avatar asset after '+boundary+' cannot hydrate previous speech or frequencies',async t=>{
  const {h,turn}=await speakingFixture(t,{avatarAvailable:false});h.sample(turn,.68);
  if(boundary==='pause')h.button('Pause animation').click();else if(boundary==='blocked'){h.client.playbackBlocked=true;h.config.onPlaybackBlocked();}else if(boundary==='unavailable')h.meter('unavailable');else if(boundary==='input')h.speech('input-2',2,'Which option?');else if(boundary==='hidden')h.hide(true);else{h.playback(turn,{playing:false});h.state('listening');}
  await h.loadAvatar();assert.equal(h.sink.level,0);assert.equal(h.calls.signals.some(value=>value?.amplitude>0),false);
  if(boundary==='pause'){h.button('Resume animation').click();assert.equal(h.sink.level,0,'resume does not replay old bins');h.sample(turn,.29);assert.equal(h.sink.level,.29);}
});

test('silence closes the signal without changing inferred conversational emotion',async t=>{
  const {h,turn}=await speakingFixture(t);h.sample(turn,.59);const emotions=h.calls.emotions.length;
  h.sample(turn,0,{outputSignal:ZERO,outputTimeMs:1100});assert.equal(h.sink.level,0);assert.equal(h.sink.currentSignal,null);assert.equal(h.calls.emotions.length,emotions);
  h.sample(turn,.75,{input:.99,outputTimeMs:1300});assert.equal(h.calls.emotions.length,emotions,'amplitude and microphone energy are not emotional evidence');
});

for(const blocked of ['paused','unavailable','blocked'])test(blocked+' output feedback clears measured bins while keeping the native session',async t=>{
  const {h,turn}=await speakingFixture(t);h.sample(turn,.64);
  if(blocked==='blocked'){h.client.playbackBlocked=true;h.config.onPlaybackBlocked();}else h.meter(blocked);
  assert.equal(h.sink.level,0);h.sample(turn,.9);assert.equal(h.sink.level,0);assert.equal(h.calls.starts,1);assert.equal(h.calls.stops,0);
  if(blocked==='blocked'){h.client.playbackBlocked=false;h.config.onPlaybackResumed();}else h.meter('ready');
  assert.equal(h.sink.level,0,'feedback recovery does not replay the previous waveform');h.sample(turn,.27);assert.equal(h.sink.level,.27);
});

test('animation pause discards incoming waveform updates until a fresh sample after resume',async t=>{
  const {h,turn}=await speakingFixture(t);h.sample(turn,.6);h.button('Pause animation').click();h.sample(turn,.9);assert.equal(h.sink.level,0);assert.equal(h.calls.stops,0);
  h.button('Resume animation').click();assert.equal(h.sink.level,0);h.sample(turn,.25);assert.equal(h.sink.level,.25);
});

test('hidden guide and old-session callbacks cannot revive speech in an explicit new session',async t=>{
  const {h,turn}=await speakingFixture(t);h.sample(turn,.6);const old=h.config;h.hide(true);assert.equal(h.sink.level,0);old.onLevel({output:.95,outputSignal:signal(.95),...turn,responseId:'reply-1'});assert.equal(h.sink.level,0);
  h.hide(false);await h.start();h.state('speaking');old.onLevel({output:.95,outputSignal:signal(.95),...turn,responseId:'reply-1'});assert.equal(h.sink.level,0,'no current native buffer exists in the new session');
  const fresh=h.speech('input-2',2);h.speak(fresh,'reply-2');h.sample(fresh,.33,{responseId:'reply-2'});assert.equal(h.sink.level,.33);assert.equal(h.calls.starts,2);
});

test('pagehide disposes the old client and its callbacks cannot drive the new client',async t=>{
  const {h,turn}=await speakingFixture(t);h.sample(turn,.6);const old=h.config;h.w.dispatchEvent(new h.w.Event('pagehide'));await settle();assert.equal(h.sink.level,0);
  await h.start();const fresh=h.speech('input-2',2);h.speak(fresh,'reply-2');h.sample(fresh,.35,{responseId:'reply-2'});const count=h.calls.signals.length;
  old.onLevel({output:.95,outputSignal:signal(.95),...turn,responseId:'reply-1'});assert.equal(h.calls.signals.length,count);assert.equal(h.sink.level,.35);assert.equal(h.calls.disposes,1);
});

test('legacy rigs receive current measured RMS and a newer legacy buffer can replace the old binding',async t=>{
  const {h,turn}=await speakingFixture(t,{legacyRig:true,nativeGetter:false});h.config.onLevel({input:0,output:.42,...turn,responseId:'reply-1'});assert.equal(h.sink.level,.42);
  h.playback(turn,{responseId:'reply-2'});h.config.onLevel({input:0,output:.28,...turn,responseId:'reply-2'});assert.equal(h.sink.level,.28);
  h.config.onLevel({input:.99,output:0,...turn,responseId:'reply-2'});assert.equal(h.sink.level,0);
});

test('malformed or explicitly unavailable spectra cannot animate fabricated frequency feedback',async t=>{
  const {h,turn}=await speakingFixture(t);h.sample(turn,.54);
  for(const bad of [{...signal(.8),bands:[.3,.2]},{...signal(.8),brightness:Infinity},{...signal(.8),amplitude:NaN},{...signal(.8),bands:[.1,.2,.3,.4,.5,2]},ZERO]){
    h.sample(turn,.9,{outputSignal:bad});assert.equal(h.sink.level,0);assert.equal(h.sink.currentSignal,null);
  }
  assert.equal(h.calls.starts,1);assert.equal(h.calls.stops,0);
});

for(const missing of ['responseId','inputItemId','turnVersion','currentTurn'])test('a modern positive signal missing '+missing+' cannot borrow the native buffer identity',async t=>{
  const {h,turn}=await speakingFixture(t);h.sample(turn,.47);const count=h.calls.signals.length;
  const sample={output:.95,outputSignal:signal(.95),...turn,responseId:'reply-1'};delete sample[missing];h.config.onLevel(sample);
  assert.equal(h.calls.signals.length,count);assert.equal(h.sink.level,.47);
});

test('a positive spectrum with zero legacy RMS cannot resurrect a stopped native buffer',async t=>{
  const {h,turn}=await speakingFixture(t);h.sample(turn,.55);h.playback(turn,{playing:false});
  const count=h.calls.signals.length;h.config.onLevel({input:0,output:0,outputSignal:signal(.9)});
  assert.equal(h.calls.signals.length,count);assert.equal(h.sink.level,0);assert.equal(h.sink.currentSignal,null);
});

test('modern native-buffer evidence retains speech despite an unrelated listening state callback',async t=>{
  const {h,turn}=await speakingFixture(t);h.sample(turn,.51);h.state('listening');
  assert.equal(h.sink.state,'speaking');assert.equal(h.sink.level,.51);assert.equal(h.calls.starts,1);
  h.playback(turn,{playing:false});h.state('listening');assert.equal(h.sink.state,'listening');assert.equal(h.sink.level,0);
});

test('legacy listening state retires the previous buffer and late identified levels cannot reopen it',async t=>{
  const {h,turn}=await speakingFixture(t,{nativeGetter:false});h.sample(turn,.53);h.state('listening');
  assert.equal(h.sink.state,'listening');assert.equal(h.sink.level,0);const count=h.calls.signals.length;
  h.sample(turn,.94);assert.equal(h.calls.signals.length,count);assert.equal(h.sink.level,0);
});

for(const late of [false,true])test('reduced motion clears waveform immediately and cannot replay it on restoration'+(late?' before avatar joins':''),async t=>{
  const {h,turn}=await speakingFixture(t,{avatarAvailable:!late});h.sample(turn,.56);h.reduced(true);
  if(late)await h.loadAvatar();assert.equal(h.sink.level,0);h.sample(turn,.95);assert.equal(h.sink.level,0);
  h.reduced(false);assert.equal(h.sink.level,0,'preference restoration cannot replay previous bins');h.sample(turn,.28);assert.equal(h.sink.level,.28);assert.equal(h.calls.stops,0);
});
