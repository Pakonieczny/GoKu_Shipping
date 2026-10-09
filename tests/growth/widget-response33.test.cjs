'use strict';
// Real widget and expression planner at the adapter/rig boundary. Exact turn,
// source failure, reload and reset regressions use synthetic callbacks; this
// does not establish physical microphone, audible media or GPU acceptance.
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs');
const {JSDOM,VirtualConsole}=require('jsdom');
const Expression=require('../../brites-concierge-expression.js');
const Core=require('../../netlify/functions/_britesGrowth.js');
const source=fs.readFileSync(require.resolve('../../brites-concierge.js'),'utf8');
const actions=fs.readFileSync(require.resolve('../../brites-concierge-voice-actions.js'),'utf8');
const settle=async()=>{await new Promise(resolve=>setImmediate(resolve));await new Promise(resolve=>setImmediate(resolve));};
const product={id:'gid://shopify/Product/1',handle:'compass-necklace',url:'https://britesjewelry.com/products/compass-necklace',title:'Compass Necklace',type:'Necklace',currency:'USD',minPrice:54,variantsComplete:true,suggestedVariantId:'gid://shopify/ProductVariant/101',variants:[{id:'gid://shopify/ProductVariant/101',numericId:'101',title:'Sterling Silver / 18 inch',price:54,available:true,options:[{name:'Metal',value:'Sterling Silver'},{name:'Length',value:'18 inch'}]},{id:'gid://shopify/ProductVariant/102',numericId:'102',title:'Sterling Silver / 20 inch',price:56,available:true,options:[{name:'Metal',value:'Sterling Silver'},{name:'Length',value:'20 inch'}]}]};
const ZERO={amplitude:0,bands:[0,0,0,0,0,0],brightness:0,valid:false};
function signal(amplitude=.4){return {amplitude,bands:[.08,.16,.35,.22,.1,.04],brightness:.27,valid:true};}

function fixture(t,{avatarAvailable=true,legacyRig=false,nativeGetter=true,expressionAvailable=true,savedPreferences={}}={}){
  const errors=[],vc=new VirtualConsole();vc.on('jsdomError',error=>errors.push(error));
  const dom=new JSDOM('<!doctype html><body></body>',{url:'https://preview.example/concierge-sandbox.html',pretendToBeVisual:true,runScripts:'outside-only',virtualConsole:vc});
  const w=dom.window,d=w.document;let hidden=false,clock=1791350000000,serial=0,controller=null,sink=null,config=null;
  Object.defineProperty(d,'hidden',{get:()=>hidden});w.Date.now=()=>clock;const motionListeners=[];const motion={matches:false,addEventListener:(name,listener)=>motionListeners.push(listener)};w.matchMedia=()=>motion;
  const timers=new Map(),calls={signals:[],levels:[],states:[],emotions:[],creations:0,starts:0,stops:0,disposes:0,requests:[],configs:[],inputs:[],expressions:[],hostResults:[]};
  w.setTimeout=(fn,ms)=>{const id=++serial;timers.set(id,{fn,at:clock+Number(ms||0)});return id;};w.clearTimeout=id=>timers.delete(id);w.HTMLElement.prototype.scrollIntoView=function(){};
  const script=d.createElement('script');script.src='/brites-concierge.js';script.dataset.sandbox='true';Object.defineProperty(d,'currentScript',{get:()=>script});
  w.sessionStorage.setItem('brites-concierge-v1',JSON.stringify({updatedAt:clock,open:false,history:[],preferences:savedPreferences,products:[product],meanings:[],policyLinks:[],productHandles:[product.handle],selectedVariants:{},uncertainVariants:[]}));
  function installExpression(){w.BritesConciergeExpression={...Expression,create(options){controller=Expression.create({...options,now:()=>clock});return controller;}};}
  if(expressionAvailable)installExpression();
  function installAvatar(){w.BritesConciergeAvatar={create(options){calls.creations++;sink={state:options.initialState,level:0,currentSignal:null,inputSignal:null,expression:null,paused:false,visible:options.visible};const api={setState(value){calls.states.push(value);if(value!==sink.state){sink.state=value;if(value!=='speaking'){sink.level=0;sink.currentSignal=null;}}},setLevel(value){calls.levels.push(value);sink.level=value;sink.currentSignal=null;},setExpression(value){calls.expressions.push(value?JSON.parse(JSON.stringify(value)):null);sink.expression=value;},setInputSignal(value){calls.inputs.push(value?JSON.parse(JSON.stringify(value)):null);sink.inputSignal=value?JSON.parse(JSON.stringify(value)):null;},setEmotion(value){calls.emotions.push(value);},setPaused(value){sink.paused=value;if(value){sink.level=0;sink.currentSignal=null;}},setVisible(value){sink.visible=value;if(!value){sink.level=0;sink.currentSignal=null;}},setFloating(){},triggerGreeting(){},cancelPerformance(){},clearFocus(){},clearProduct(){},focusProduct(){},showProduct(){return true;},cue(){},perform(){return true;},retry(){},destroy(){}};if(!legacyRig)api.setSpeechSignal=value=>{calls.signals.push(value?JSON.parse(JSON.stringify(value)):null);sink.currentSignal=value?JSON.parse(JSON.stringify(value)):null;sink.level=value?.valid===true?value.amplitude:0;return true;};return api;}};}
  if(avatarAvailable)installAvatar();
  const clients=[];
  w.BritesConciergeVoice={create(value){config=value;calls.configs.push(value);const client={state:'listening',outputMeterState:'ready',playbackBlocked:false,native:null,incoming:null,start:async()=>{calls.starts++;client.native=null;client.state='listening';return true;},stop:async()=>{calls.stops++;client.native=client.incoming=null;client.state='idle';},dispose:async()=>{calls.disposes++;client.native=client.incoming=null;client.state='idle';},interrupt(){client.native=null;client.state='listening';value.onState('listening');},updateContext(){}};if(nativeGetter)Object.defineProperty(client,'currentOutput',{get:()=>client.native?{...client.native}:null});Object.defineProperty(client,'currentInput',{get:()=>client.incoming?{...client.incoming}:null});clients.push(client);return client;}};
  w.fetch=async(raw,init={})=>{const body=init.body?JSON.parse(init.body):{};calls.requests.push({url:String(raw),body});return {ok:true,json:async()=>String(raw).includes('/api/growth/product?')?{live:true,product}:body.message?{live:true,reply:'The live piece was checked.',preferences:{},products:[product],meanings:[]}:{enabled:true}};};
  w.eval(actions);const marker='function nativeControlReceipt(result){';assert.equal(source.split(marker).length,2);w.__captureControl33=result=>calls.hostResults.push(JSON.parse(JSON.stringify(result)));w.eval(source.replace(marker,marker+'window.__captureControl33(result);'));const root=d.querySelector('brites-concierge').shadowRoot;
  const button=label=>[...root.querySelectorAll('button')].find(node=>node.textContent.trim()===label||node.getAttribute('aria-label')===label);
  const h={w,d,root,calls,errors,button,get config(){return config;},get client(){return clients.at(-1);},get sink(){return sink;},get controller(){return controller;},async loadExpression(){installExpression();const loader=root.querySelector('script[src$="brites-concierge-expression.js"]');assert.ok(loader);loader.dispatchEvent(new w.Event('load'));await settle();},async start(){w.BritesConcierge.open();await settle();button('Talk to me').click();await settle();root.querySelector('script[src$="brites-concierge-voice.js"]')?.dispatchEvent(new w.Event('load'));await settle();},advance(ms){const end=clock+ms;while(clock<end){clock=Math.min(end,clock+50);for(const [id,value] of [...timers])if(value.at<=clock){timers.delete(id);value.fn();}}},state(value){h.client.state=value;config.onState(value);},meter(value){h.client.outputMeterState=value;config.onOutputMeterState(value);},speech(itemId='input-1',turnVersion=1,text='Show options for the first one.'){h.client.native=null;h.client.incoming={speaking:true,itemId,turnVersion,currentTurn:true};config.onSpeechStarted({itemId,turnVersion,reason:'speech'});h.state('listening');config.onTranscript({role:'user',itemId,turnVersion,currentTurn:true,text,final:true});return {inputItemId:itemId,turnVersion,currentTurn:true};},playback(turn,{playing=true,responseId='reply-1',...extra}={}){const value={...turn,responseId,itemId:'output-'+responseId,playing,...extra};h.client.native=playing?{...value,currentTurn:true}:null;config.onPlaybackState(value);return value;},speak(turn,responseId='reply-1'){config.onTranscript({role:'assistant',...turn,responseId,itemId:'output-'+responseId,text:'Here is the compass design. Which length would you prefer?',final:true});h.playback(turn,{responseId});h.state('speaking');h.meter('ready');},sample(turn,amplitude=.4,extra={}){const value={input:0,output:amplitude,outputSignal:signal(amplitude),outputTimeMs:1000,outputClock:'local-media-currentTime',responseId:h.client.native?.responseId||'reply-1',...turn,...extra};config.onLevel(value);return value;},input(amplitude=.6,extra={}){config.onInputSignal({level:amplitude,signal:signal(amplitude),speaking:true,itemId:h.client.incoming?.itemId||'input-1',turnVersion:h.client.incoming?.turnVersion||1,currentTurn:true,...extra});},async loadAvatar(){installAvatar();const loader=root.querySelector('script[src$="brites-concierge-avatar.js"]');assert.ok(loader);loader.dispatchEvent(new w.Event('load'));await settle();},hide(value){hidden=value;d.dispatchEvent(new w.Event('visibilitychange'));},reduced(value){motion.matches=value;motionListeners.forEach(listener=>listener({matches:value}));}};
  t.after(()=>{try{w.BritesConcierge.close();}catch{}controller?.destroy();timers.clear();w.close();});return h;
}

async function listening(t,options){const h=fixture(t,options);await h.start();h.speech('input-1',1,'');return h;}

test('current measured input reaches the listening rig without requiring any assistant output',async t=>{
  const h=await listening(t);h.input(.65);assert.deepEqual(h.sink.inputSignal,signal(.65));assert.equal(h.sink.level,0);assert.equal(h.sink.currentSignal,null);assert.equal(h.controller.snapshot().listening,true);
  const base=h.controller.snapshot().cue.intensity;for(let i=0;i<5;i++){h.advance(50);h.config.onLevel({input:1,output:0,outputSignal:ZERO});h.input(.65);}assert.ok(h.controller.snapshot().cue.intensity>base,'independent qualified input accumulates observed attention');
});

for(const changed of [{itemId:'old-input'},{turnVersion:0},{currentTurn:false},{speaking:true,itemId:'',turnVersion:null}])test('foreign or unqualified input cannot replace current listening spectrum '+JSON.stringify(changed),async t=>{
  const h=await listening(t);h.input(.45);const before=h.calls.inputs.length;h.input(.95,changed);assert.equal(h.calls.inputs.length,before);assert.deepEqual(h.sink.inputSignal,signal(.45));
});

test('muted or ended native input cannot borrow a current turn from the callback',async t=>{
  const h=await listening(t);h.input(.7);h.client.incoming=null;h.config.onInputSignal({level:0,signal:ZERO,speaking:false,itemId:'',turnVersion:null,currentTurn:false});assert.equal(h.sink.inputSignal,null);const count=h.calls.inputs.length;h.input(.9);assert.equal(h.calls.inputs.length,count);
});

test('a current analyser failure clears input immediately while semantic listening remains active',async t=>{
  const h=await listening(t);h.input(.7);h.config.onListeningTranscript({delta:'That suggestion does not work for me.',itemId:'input-1',turnVersion:1,currentTurn:true,final:false});const cue=h.controller.snapshot().cue;
  h.input(0,{signal:ZERO});assert.equal(h.sink.inputSignal,null);assert.equal(h.controller.snapshot().listening,true);assert.equal(h.controller.snapshot().cue.kind,cue.kind);
  h.input(.6);h.config.onInputSignal({level:0,signal:ZERO,speaking:false,itemId:'input-1',turnVersion:1,currentTurn:true});assert.equal(h.sink.inputSignal,null);
});

test('an unattributed raw input on the output channel cannot fabricate listener backchannels',async t=>{
  const h=await listening(t);h.client.incoming=null;const base=h.controller.snapshot().cue;for(let i=0;i<25;i++){h.advance(50);h.config.onLevel({input:1,output:0,outputSignal:ZERO});}assert.equal(h.controller.snapshot().cue.kind,base.kind);assert.ok(h.controller.snapshot().cue.intensity<=base.intensity);assert.equal(h.sink.inputSignal,null);
});

test('output-only callbacks cannot erase the independently observed input interval',async t=>{
  const h=await listening(t);const base=h.controller.snapshot().cue.intensity;for(let i=0;i<6;i++){h.advance(50);h.config.onLevel({input:0,output:0,outputSignal:ZERO});h.input(.8);}assert.ok(h.controller.snapshot().cue.intensity>base);
});

test('listening partial semantics and final after speech-stop react without creating history or action authority from a partial',async t=>{
  const h=await listening(t);const before=JSON.parse(h.w.sessionStorage.getItem('brites-concierge-v1')).history.length;
  h.config.onListeningTranscript({delta:'That does not work for me.',itemId:'input-1',turnVersion:1,currentTurn:true,final:false});assert.equal(h.controller.snapshot().cue.kind,'reflect');assert.equal(JSON.parse(h.w.sessionStorage.getItem('brites-concierge-v1')).history.length,before);
  h.client.incoming=null;h.state('thinking');h.config.onTranscript({role:'user',text:'Actually, I love that one.',itemId:'input-1',turnVersion:1,currentTurn:true,final:true});assert.equal(h.controller.snapshot().cue.kind,'appreciate');assert.equal(h.controller.snapshot().listening,false);
  h.advance(2450);assert.equal(h.controller.snapshot().cue,null);assert.equal(h.w.sessionStorage.getItem('brites-sandbox-cart'),null);
});

test('stale partial semantics cannot contaminate the newer listening turn',async t=>{
  const h=await listening(t);h.speech('input-2',2,'');const before=h.controller.snapshot().cue;h.config.onListeningTranscript({delta:'I am grieving.',itemId:'input-1',turnVersion:1,currentTurn:true,final:false});assert.deepEqual(h.controller.snapshot().cue,before);assert.equal(h.controller.snapshot().context,'ordinary');
});

test('listening input never erases or becomes a speaking mouth signal',async t=>{
  const h=await listening(t);const turn={inputItemId:'input-1',turnVersion:1,currentTurn:true};h.client.incoming=null;h.speak(turn);h.sample(turn,.55);const cue=h.controller.snapshot().cue;h.config.onInputSignal({level:0,signal:ZERO,speaking:false,itemId:'',turnVersion:null,currentTurn:false});assert.deepEqual(h.sink.currentSignal,signal(.55));assert.equal(h.sink.level,.55);assert.deepEqual(h.controller.snapshot().cue,cue);assert.equal(h.sink.inputSignal,null);
});

for(const reason of ['pause','reduced','hide','stop','new-turn'])test('listening source is cleared at '+reason+' and cannot be revived by stale callbacks',async t=>{
  const h=await listening(t);h.input(.7);if(reason==='pause')h.button('Pause animation').click();else if(reason==='reduced')h.reduced(true);else if(reason==='hide')h.hide(true);else if(reason==='stop'){h.button('End voice').click();await settle();}else h.speech('input-2',2,'');assert.equal(h.sink.inputSignal,null);h.input(.9,{itemId:'input-1',turnVersion:1});assert.equal(h.sink.inputSignal,null);
});

test('late avatar joins a fresh exact input sample once but cannot replay an expired one',async t=>{
  const h=await listening(t,{avatarAvailable:false});h.input(.4);h.advance(260);await h.loadAvatar();assert.equal(h.sink.inputSignal,null);h.input(.6);assert.deepEqual(h.sink.inputSignal,signal(.6));
});

test('late avatar does not extend the original microphone observation expiry',async t=>{
  const h=await listening(t,{avatarAvailable:false});h.input(.6);h.advance(200);await h.loadAvatar();assert.deepEqual(h.sink.inputSignal,signal(.6));h.advance(55);assert.equal(h.sink.inputSignal,null);
});

test('late expression asset cannot turn old input energy into a new observation',async t=>{
  const h=await listening(t,{expressionAvailable:false});h.input(.8);h.advance(1000);await h.loadExpression();assert.equal(h.controller.snapshot().cue.kind,'attentive');assert.ok(h.controller.snapshot().cue.intensity<=.28);assert.equal(h.sink.inputSignal,null);
});

test('Start fresh restores the full host collection and preserves the separate sandbox bag',async t=>{
  const h=fixture(t);const commands=[];h.w.BritesSandboxStorefront={execute:async action=>{commands.push(action);return {executed:true};}};const bag='[{"variantId":"101","title":"Existing piece"}]';h.w.sessionStorage.setItem('brites-sandbox-cart',bag);h.w.BritesConcierge.open();await settle();h.button('Start fresh').click();await settle();assert.deepEqual(JSON.parse(JSON.stringify(commands)),[{type:'filter',filter:'all'}]);assert.equal(h.w.sessionStorage.getItem('brites-sandbox-cart'),bag);assert.deepEqual(JSON.parse(h.w.sessionStorage.getItem('brites-concierge-v1')).products,[]);
});

function collection(h,filter,search,rows){
  h.w.BritesStorefrontBridge=require('../../brites-storefront-bridge.js');
  const page={pageKind:'collection',contextRevision:1,discoveryRevision:1,filter,search,sort:'featured',loading:false,currentHandle:'',focusedHandle:'',visiblePieces:rows.map(p=>({id:p.id,handle:p.handle,title:p.title}))};
  h.w.BritesSandboxStorefront={snapshot:()=>page,execute:async()=>({ok:true})};
  const publish=()=>{page.contextRevision++;h.d.dispatchEvent(new h.w.CustomEvent('brites-storefront:context'));};publish();return {page,publish};
}
const stored=h=>JSON.parse(h.w.sessionStorage.getItem('brites-concierge-v1'));
const earrings=(id,title)=>({...product,id:'gid://shopify/Product/'+id,handle:'moon-earrings-'+id,url:'https://britesjewelry.com/products/moon-earrings-'+id,title,type:'Earrings'});

test('a completed category switch clears obsolete discovery but keeps the shopper’s stable constraints',async t=>{
  const preferences=Core.intentFrom('A bunny silver necklace under 80 for my mother’s birthday'),h=fixture(t,{savedPreferences:preferences});
  const view=collection(h,'earrings','',[earrings(2,'Moon Earrings')]),saved=stored(h);
  assert.equal(saved.preferences.query,'');assert.equal(saved.preferences.type,'earrings');assert.deepEqual(saved.preferences.interests,[]);assert.deepEqual(saved.preferences.excludedInterests,[]);
  for(const field of ['metal','budget','budgetCurrency','recipient','occasion'])assert.equal(saved.preferences[field],preferences[field],field);
  assert.deepEqual(saved.productHandles,['moon-earrings-2']);assert.deepEqual(saved.products,[],'the old necklace tray is retired');
  const before=JSON.stringify(saved.preferences);view.page.focusedHandle='moon-earrings-2';view.publish();assert.equal(JSON.stringify(stored(h).preferences),before,'pointer-only context is not a discovery reset');
});

test('full collection and new search transitions replace stale scope only after the completed view',async t=>{
  const h=fixture(t,{savedPreferences:Core.intentFrom('A bunny necklace under 80')}),view=collection(h,'necklaces','bunny',[product]);
  view.page.loading=true;view.page.filter='earrings';view.page.search='moon';view.publish();assert.equal(stored(h).preferences.type,'necklace');assert.equal(stored(h).preferences.query,'bunny');
  view.page.loading=false;view.publish();assert.equal(stored(h).preferences.type,'necklace','an aborted preview with the same completed revision is not adopted');assert.equal(stored(h).preferences.query,'bunny');
  view.page.discoveryRevision++;view.page.visiblePieces=[{id:'gid://shopify/Product/2',handle:'moon-earrings-2',title:'Moon Earrings'}];view.publish();assert.equal(stored(h).preferences.type,'earrings');assert.equal(stored(h).preferences.query,'moon');
  view.page.discoveryRevision++;view.page.filter='all';view.page.search='';view.publish();assert.equal(stored(h).preferences.type,null);assert.equal(stored(h).preferences.query,'');assert.equal(stored(h).preferences.budget,80);
});

test('meaning and comparison after a category switch check the currently displayed earrings through the production core',async t=>{
  const h=fixture(t,{savedPreferences:Core.intentFrom('A bunny silver necklace under 80 for my mother')}),rows=[earrings(2,'Moon Earrings'),earrings(3,'Star Earrings')],byHandle=[],requests=[];
  collection(h,'earrings','',rows);
  const service={saveProducts:async()=>{},productIssues:async()=>[],research:async()=>[],storySupplements:async()=>[]};
  const shopify={search:async()=>{throw Error('No discovery search expected for displayed meanings');},byHandle:async handle=>{byHandle.push(handle);const p=rows.find(p=>p.handle===handle);return p?{...p,description:p.title,tags:[],options:[],images:[],checkedAt:1791350000000}:null;}};
  h.w.fetch=async(raw,init={})=>{const body=init.body?JSON.parse(init.body):{};if(!body.message)return {ok:true,json:async()=>({enabled:true})};requests.push(body);const answer=await Core.concierge({service,shopify,...body,now:()=>1791350000000});return {ok:true,json:async()=>answer};};
  h.w.BritesConcierge.open();await settle();h.button('Type instead').click();
  for(const message of ['Tell me the meaning of these','Compare these pieces']){
    const input=h.root.querySelector('form input');input.value=message;h.root.querySelector('form').dispatchEvent(new h.w.Event('submit',{bubbles:true,cancelable:true}));for(let i=0;i<8;i++)await settle();
    assert.deepEqual(stored(h).products.map(p=>p.handle),rows.map(p=>p.handle),JSON.stringify({message,requests,reply:stored(h).latestReply,preferences:stored(h).preferences,errors:h.errors.map(e=>e.message)}));
  }
  assert.equal(requests.length,2);assert.ok(requests.every(r=>r.preferences.type==='earrings'&&r.preferences.query===''));assert.deepEqual(byHandle,['moon-earrings-2','moon-earrings-3','moon-earrings-2','moon-earrings-3']);
});

test('explicit broad reset retires model-installed preferences even when search and filter labels are unchanged',async t=>{
  const h=fixture(t),view=collection(h,'all','',[product]),preferences=Core.intentFrom('A bunny necklace for my mother');
  h.w.BritesSandboxStorefront.presentProducts=rows=>{view.page.discoveryRevision++;view.page.visiblePieces=rows.map(p=>({id:p.id,handle:p.handle,title:p.title}));view.publish();return {ok:true};};
  h.w.fetch=async(raw,init={})=>{const body=init.body?JSON.parse(init.body):{};return {ok:true,json:async()=>body.message?{live:true,reply:'Here is one checked piece.',preferences,products:[product],meanings:[]}:{enabled:true}};};
  h.w.BritesConcierge.open();await settle();h.button('Type instead').click();h.root.querySelector('form input').value='A bunny necklace for my mother';h.root.querySelector('form').dispatchEvent(new h.w.Event('submit',{bubbles:true,cancelable:true}));for(let i=0;i<8;i++)await settle();
  assert.equal(stored(h).preferences.query,'bunny');assert.equal(stored(h).preferences.type,'necklace');
  view.page.discoveryRevision++;view.page.visiblePieces=[{id:'gid://shopify/Product/2',handle:'moon-earrings-2',title:'Moon Earrings'}];view.publish();
  assert.equal(stored(h).preferences.query,'');assert.equal(stored(h).preferences.type,null);assert.equal(stored(h).preferences.recipient,'mother');assert.deepEqual(stored(h).productHandles,['moon-earrings-2']);
});

for(const revision of [0,-1,1.5,'2',Number.MAX_SAFE_INTEGER+1])test('an uncompleted or invalid discovery revision cannot rewrite confirmed preferences '+revision,async t=>{
  const h=fixture(t,{savedPreferences:Core.intentFrom('A bunny necklace')}),view=collection(h,'necklaces','bunny',[product]);view.page.discoveryRevision=revision;view.page.filter='earrings';view.page.search='moon';view.publish();assert.equal(stored(h).preferences.type,'necklace');assert.equal(stored(h).preferences.query,'bunny');
});

test('a finalized native category action stays authorized while its own completed host view publishes',async t=>{
  const h=fixture(t),row=earrings(2,'Moon Earrings'),view=collection(h,'necklaces','bunny',[product]);await h.start();
  h.speech('input-1',1,'');h.client.incoming=null;h.config.onTranscript({role:'user',text:'Show all earrings',itemId:'input-1',turnVersion:1,currentTurn:true,final:true,committed:true});
  h.w.BritesSandboxStorefront.execute=async action=>{assert.equal(action.filter,'earrings');view.page.filter='earrings';view.page.search='';view.page.discoveryRevision++;view.page.visiblePieces=[{id:row.id,handle:row.handle,title:row.title}];view.publish();return {ok:true,live:true,checkedAt:1791350000000,products:[row],snapshot:view.page,message:'Earrings are ready.'};};
  const result=await h.config.onTool({type:'filter',filter:'earrings'},{name:'control_storefront',inputItemId:'input-1',turnVersion:1,currentTurn:true,responseId:'reply-1'});assert.equal(result.ok,true,JSON.stringify(result));assert.equal(stored(h).preferences.type,'earrings');assert.equal(stored(h).preferences.query,'');assert.deepEqual(stored(h).products.map(p=>p.handle),[row.handle]);
});

test('a completed manual category cancels a pending older core answer without replaying its products',async t=>{
  const h=fixture(t),view=collection(h,'all','',[product]);let release,presented=0;const pending=new Promise(resolve=>{release=resolve;});
  h.w.BritesSandboxStorefront.presentProducts=()=>{presented++;};h.w.fetch=async(raw,init={})=>{const body=init.body?JSON.parse(init.body):{};return body.message?pending:{ok:true,json:async()=>({enabled:true})};};
  h.w.BritesConcierge.open();await settle();h.button('Type instead').click();h.root.querySelector('form input').value='A bunny necklace for my mother';h.root.querySelector('form').dispatchEvent(new h.w.Event('submit',{bubbles:true,cancelable:true}));await settle();assert.equal(h.root.querySelector('form input').disabled,true);
  view.page.filter='earrings';view.page.discoveryRevision++;view.page.visiblePieces=[{id:'gid://shopify/Product/2',handle:'moon-earrings-2',title:'Moon Earrings'}];view.publish();assert.equal(h.root.querySelector('form input').disabled,false);
  release({ok:true,json:async()=>({live:true,reply:'Old necklace answer.',preferences:Core.intentFrom('A bunny necklace'),products:[product],meanings:[]})});for(let i=0;i<8;i++)await settle();
  assert.equal(presented,0);assert.equal(stored(h).preferences.type,'earrings');assert.equal(stored(h).preferences.query,'');assert.deepEqual(stored(h).productHandles,['moon-earrings-2']);assert.doesNotMatch(stored(h).latestReply,/Old necklace answer/);
});

test('a successful manual discovery deferred behind a failed native bridge is adopted on completion',async t=>{
  const h=fixture(t),view=collection(h,'necklaces','bunny',[product]);await h.start();h.speech('input-1',1,'');h.client.incoming=null;h.config.onTranscript({role:'user',text:'Show all rings',itemId:'input-1',turnVersion:1,currentTurn:true,final:true});
  let release;const commands=[],pending=new Promise(resolve=>{release=resolve;});h.w.BritesSandboxStorefront.execute=async action=>{commands.push(JSON.parse(JSON.stringify(action)));return pending;};
  const result=h.config.onTool({type:'filter',filter:'rings'},{name:'control_storefront',inputItemId:'input-1',turnVersion:1,currentTurn:true,responseId:'reply-1'});await settle();
  view.page.filter='earrings';view.page.search='';view.page.discoveryRevision++;view.page.visiblePieces=[{id:'gid://shopify/Product/2',handle:'moon-earrings-2',title:'Moon Earrings'}];view.publish();assert.equal(stored(h).preferences.type,'necklace');
  release({ok:false,message:'The old request was canceled.'});const packet=await result;assert.ok(h.calls.hostResults.at(-1).error);assert.equal(h.calls.hostResults.at(-1).ok,false);assert.equal(packet.ok,false);assert.equal(typeof packet.reply,'string');assert.ok(packet.reply.trim());assert.equal(packet.customerMessage,packet.reply);assert.equal(packet.cartChanged,false);assert.doesNotMatch(JSON.stringify(packet),/"(?:error|publicContext|completedActions|stepResults|preferences|action|actions)"|\b(?:backend|authority|postcondition|metadata)\b/i);assert.deepEqual(commands,[{type:'filter',filter:'rings'}]);assert.equal(stored(h).preferences.type,'earrings');assert.equal(stored(h).preferences.query,'');assert.deepEqual(stored(h).productHandles,['moon-earrings-2']);assert.equal(view.page.filter,'earrings');assert.equal(h.root.querySelector('form input').disabled,false);
});
