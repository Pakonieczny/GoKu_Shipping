'use strict';
// Actual widget + live-product projection, with synthetic browser callbacks.
// No microphone, provider, audible-media, live-store or GPU claim is made.
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs');
const {JSDOM,VirtualConsole}=require('jsdom');
const Bridge=require('../../brites-storefront-bridge.js');
const source=fs.readFileSync(require.resolve('../../brites-concierge.js'),'utf8');
const actions=fs.readFileSync(require.resolve('../../brites-concierge-voice-actions.js'),'utf8');
const clone=value=>JSON.parse(JSON.stringify(value));
const settle=async()=>{await new Promise(resolve=>setImmediate(resolve));await new Promise(resolve=>setImmediate(resolve));};
async function bounded(promise,label){let timer;try{return await Promise.race([promise,new Promise((resolve,reject)=>{timer=setTimeout(()=>reject(Error(label+' did not settle within 2500 ms.')),2500);})]);}finally{clearTimeout(timer);}}
function deferred(){let resolve;const promise=new Promise(r=>{resolve=r;});return {promise,resolve};}
function piece(id,kind='earrings'){const handle='moon-'+kind+'-'+id;return {id:'gid://shopify/Product/'+id,handle,title:'Moon '+kind+' '+id,url:'https://britesjewelry.com/products/'+handle,type:kind,currency:'USD',minPrice:54,description:'Sterling silver moon design.',variantsComplete:true,variants:[{id:'gid://shopify/ProductVariant/'+id+'01',numericId:id+'01',title:'Sterling Silver',price:54,available:true,options:[{name:'Metal',value:'Sterling Silver'}]}]};}
const old=piece(1,'necklace'),current=piece(20),visible=Array.from({length:6},(_,i)=>piece(20+i));
const signal={amplitude:.48,bands:[.08,.16,.35,.22,.1,.04],brightness:.27,valid:true};

function fixture(t){
  const errors=[],vc=new VirtualConsole();vc.on('jsdomError',value=>errors.push(value));
  const dom=new JSDOM('<!doctype html><body></body>',{url:'https://preview.example/concierge-sandbox.html',runScripts:'outside-only',pretendToBeVisual:true,virtualConsole:vc});
  const w=dom.window,d=w.document,clock=1791350000000,timers=new Map();let serial=0,config,hidden=false,readGate=null,live={live:true,product:clone(current)},voiceTurn=0,activeInputItemId='read-input',activeTurnVersion=1;
  w.Date.now=()=>clock;Object.defineProperty(d,'hidden',{get:()=>hidden});w.matchMedia=()=>({matches:false,addEventListener(){}});w.HTMLElement.prototype.scrollIntoView=function(){};
  w.setTimeout=(fn,ms)=>{const id=++serial;timers.set(id,{fn,ms});return id;};w.clearTimeout=id=>timers.delete(id);
  const script=d.createElement('script');script.src='/brites-concierge.js';script.dataset.sandbox='true';Object.defineProperty(d,'currentScript',{get:()=>script});
  w.sessionStorage.setItem('brites-concierge-v1',JSON.stringify({open:false,updatedAt:clock,history:[],preferences:{query:'bunny',interests:['bunny'],type:'necklace'},products:[old],productHandles:[old.handle],meanings:[],policyLinks:[]}));
  let page={pageKind:'collection',contextRevision:1,discoveryRevision:1,currentHandle:'',focusedHandle:'',search:'bunny',filter:'necklaces',loading:false,visiblePieces:[{id:old.id,handle:old.handle,title:old.title}]};
  const calls={reads:[],requests:[],states:[],signals:[],contexts:[],navigations:[],outcomes:[]},sink={state:'idle',signal:null};
  // This fixture supplies read-only identities. It has no page executor or
  // option controls that could truthfully confirm a requested preparation.
  w.BritesSandboxStorefront={snapshot:()=>clone(page)};w.BritesSandboxNavigate=handle=>calls.navigations.push(handle);
  w.BritesConciergeAvatar={create(){return {setState(value){sink.state=value;calls.states.push(value);},setSpeechSignal(value){sink.signal=clone(value);calls.signals.push(clone(value));return true;},setLevel(){},setEmotion(){},setInputSignal(){},setPaused(){},setVisible(){},setFloating(){},triggerGreeting(){},cancelPerformance(){},clearFocus(){},clearProduct(){},focusProduct(){},showProduct(){return true;},cue(){},perform(){return true;},retry(){},destroy(){}};}};
  const client={state:'listening',outputMeterState:'ready',playbackBlocked:false,native:null,start:async()=>true,stop:async()=>{client.native=null;},dispose:async()=>{},updateContext(value){calls.contexts.push(clone(value));}};Object.defineProperty(client,'currentOutput',{get:()=>client.native&&clone(client.native)});
  w.BritesConciergeVoice={create(value){config=value;return client;}};
  w.fetch=async(raw,init={})=>{const entry={url:String(raw),signal:init.signal,body:init.body?JSON.parse(init.body):{}};calls.requests.push(entry);if(entry.url.includes('/api/growth/product?')){calls.reads.push(entry);if(readGate)await readGate.promise;return {ok:live.ok!==false,json:async()=>clone(live)};}return {ok:true,json:async()=>({enabled:true})};};
  w.eval(fs.readFileSync(require.resolve('../../brites-storefront-bridge.js'),'utf8'));w.eval(actions);w.eval(source);const root=d.querySelector('brites-concierge').shadowRoot;
  const button=label=>[...root.querySelectorAll('button')].find(value=>value.textContent.trim()===label||value.getAttribute('aria-label')===label);
  function publish(changes={}){page={...page,...clone(changes),contextRevision:page.contextRevision+1};d.dispatchEvent(new w.CustomEvent('brites-storefront:context'));}
  const h={w,d,root,calls,sink,client,errors,button,publish,get config(){return config;},get page(){return clone(page);},setLive(value){live=clone(value);},gate(value){readGate=value;},saved(){return JSON.parse(w.sessionStorage.getItem('brites-concierge-v1'));},async tool(name,args,context={}){const result=await bounded(config.onTool(args,{name,currentTurn:true,inputItemId:activeInputItemId,turnVersion:activeTurnVersion,responseId:'read-reply-'+activeTurnVersion,...context}),'The actual '+name+' callback');calls.outcomes.push({name,args:clone(args),result:clone(result)});return result;},speech(text){activeInputItemId='read-input-'+(++voiceTurn);activeTurnVersion=voiceTurn;config.onSpeechStarted({itemId:activeInputItemId,turnVersion:activeTurnVersion,reason:'speech'});config.onTranscript({role:'user',itemId:activeInputItemId,turnVersion:activeTurnVersion,currentTurn:true,text,final:true});return {currentTurn:true,inputItemId:activeInputItemId,turnVersion:activeTurnVersion};},speaking(){const turn={currentTurn:true,inputItemId:activeInputItemId,turnVersion:activeTurnVersion,responseId:'read-reply-'+activeTurnVersion,itemId:'read-output',playing:true};client.native=turn;client.state='speaking';config.onPlaybackState(turn);config.onState('speaking');config.onOutputMeterState('ready');config.onLevel({...turn,input:0,output:signal.amplitude,outputSignal:clone(signal),outputTimeMs:900,outputClock:'local-media-currentTime'});},hide(){hidden=true;d.dispatchEvent(new w.Event('visibilitychange'));},async ready(){w.BritesConcierge.open();await settle();button('Talk to me').click();await settle();root.querySelector('script[src$="brites-concierge-voice.js"]')?.dispatchEvent(new w.Event('load'));await settle();publish({discoveryRevision:2,filter:'earrings',search:'',visiblePieces:visible.map(({id,handle,title})=>({id,handle,title}))});await settle();assert.equal(root.querySelectorAll('.card').length,0);assert.equal(h.saved().products.length,0);}};
  t.after(()=>{try{w.BritesConcierge.close();}catch{}timers.clear();w.close();});return h;
}

test('a unique current completed storefront identity receives fresh exact fact and option reads without a widget card',async t=>{
  const h=fixture(t);await h.ready();h.setLive({live:true,product:{...clone(current),title:'Current Moon Earrings',description:'Current sterling silver finish.',variants:current.variants.map(value=>({...clone(value),price:67}))}});
  const before=h.saved(),checked=await h.tool('inspect_jewellery',{handle:current.handle});
  assert.equal(checked.live,true);assert.equal(checked.product.id,current.id);assert.equal(checked.product.handle,current.handle);assert.equal(checked.product.title,'Current Moon Earrings');assert.equal(checked.product.minPrice,67);assert.equal(checked.product.currency,'USD');assert.equal(checked.product.description,'Current sterling silver finish.');assert.equal(checked.product.variants[0].id,current.variants[0].id);assert.deepEqual([...checked.actions],[]);
  assert.equal(h.calls.reads.length,1);assert.equal(new URL(h.calls.reads[0].url).searchParams.get('handle'),current.handle);assert.equal(h.root.querySelectorAll('.card').length,0);assert.deepEqual(h.saved(),before);assert.equal(h.w.sessionStorage.getItem('brites-sandbox-cart'),null);assert.deepEqual(h.calls.navigations,[]);assert.deepEqual(h.errors,[]);
});

test('read-only current visible inspection preserves an actual native output buffer and its measured mouth spectrum',async t=>{
  const h=fixture(t);await h.ready();h.speech('What metal is used for '+current.title+'?');h.speaking();const buffer=clone(h.client.currentOutput),states=h.calls.states.length,signals=h.calls.signals.length,caption=h.root.querySelector('.caption-text').textContent;
  const checked=await h.tool('inspect_jewellery',{handle:current.handle});assert.equal(checked.live,true);assert.deepEqual(h.client.currentOutput,buffer);assert.equal(h.sink.state,'speaking');assert.deepEqual(h.sink.signal,signal);assert.equal(h.calls.states.length,states);assert.equal(h.calls.signals.length,signals);assert.equal(h.root.querySelector('.caption-text').textContent,caption);
});

for(const [name,changes,handle] of [
  ['duplicate handle',{visiblePieces:[{id:current.id,handle:current.handle,title:'A'},{id:'gid://shopify/Product/99',handle:current.handle,title:'B'}]}],
  ['duplicate exact identity',{visiblePieces:[{id:current.id,handle:current.handle,title:'A'},{id:current.id,handle:current.handle,title:'B'}]}],
  ['duplicate id under another handle',{visiblePieces:[{id:current.id,handle:current.handle,title:'A'},{id:current.id,handle:'other-earrings',title:'B'}]}],
  ['loading collection',{loading:true}],
  ['uncompleted discovery',{discoveryRevision:0}],
  ['nonvisible identity',{visiblePieces:[{id:'gid://shopify/Product/99',handle:'other-earrings',title:'Other'}]}],
  ['unrelated page',{pageKind:'bag'}],
  ['foreign handle syntax',{},'https://foreign.example/products/'+current.handle],
  ['path traversal',{},'../'+current.handle]
])test(name+' cannot supply current visible product read authority',async t=>{
  const h=fixture(t);await h.ready();h.publish(changes);const result=await h.tool('inspect_jewellery',{handle:handle||current.handle});assert.ok(result.error);assert.equal(result.product,undefined);assert.equal(h.calls.reads.length,0);assert.equal(h.root.querySelectorAll('.card').length,0);assert.equal(h.w.sessionStorage.getItem('brites-sandbox-cart'),null);
});

for(const [name,change] of [
  ['mismatched product id',{id:'gid://shopify/Product/99'}],
  ['mismatched product handle',{handle:'another-earrings'}],
  ['foreign product URL',{url:'https://foreign.example/products/'+current.handle}],
  ['wrong owned product URL',{url:'https://britesjewelry.com/products/other-earrings'}],
  ['credential-bearing product URL',{url:'https://user:password@britesjewelry.com/products/'+current.handle}],
  ['held product',{cartHold:true}],
  ['parts-only product',{partsOnly:true}],
  ['missing exact options',{variants:[]}]
])test(name+' is rejected by the real live-product projection after a visible identity read',async t=>{
  const h=fixture(t);await h.ready();h.setLive({live:true,product:{...clone(current),...change}});const checked=await h.tool('inspect_jewellery',{handle:current.handle});assert.ok(checked.error);assert.equal(checked.product,undefined);assert.equal(h.calls.reads.length,1);assert.equal(h.saved().products.length,0);assert.equal(h.w.sessionStorage.getItem('brites-sandbox-cart'),null);
});

for(const [name,changes] of [
  ['new completed discovery with the same visible identity',{discoveryRevision:3}],
  ['changed current page context',{focusedHandle:visible[1].handle}],
  ['identity removed before completion',{visiblePieces:visible.slice(1).map(({id,handle,title})=>({id,handle,title}))}],
  ['current collection became loading',{loading:true}],
  ['current identity changed id',{visiblePieces:[{id:'gid://shopify/Product/99',handle:current.handle,title:current.title}]}]
])test(name+' invalidates an in-flight storefront-only read',async t=>{
  const h=fixture(t);await h.ready();const gate=deferred();h.gate(gate);const pending=h.tool('inspect_jewellery',{handle:current.handle});await settle();assert.equal(h.calls.reads.length,1);h.publish(changes);gate.resolve();const result=await pending;assert.ok(result.error);assert.equal(result.product,undefined);assert.equal(h.saved().products.length,0);assert.equal(h.root.querySelectorAll('.card').length,0);assert.equal(h.w.sessionStorage.getItem('brites-sandbox-cart'),null);
});

test('external cancellation and a closed voice session cannot publish a storefront-only read',async t=>{
  const h=fixture(t);await h.ready();const gate=deferred(),abort=new AbortController();h.gate(gate);const pending=h.tool('inspect_jewellery',{handle:current.handle},{signal:abort.signal});await settle();abort.abort();h.w.BritesConcierge.close();gate.resolve();const result=await pending;assert.ok(result.error);assert.equal(result.product,undefined);assert.equal(h.root.querySelectorAll('.card').length,0);
});

for(const action of ['view','options','review'])test('identity-only host context cannot prepare '+action+' even after a fresh verified inspection',async t=>{
  const h=fixture(t);await h.ready();const inspection=h.speech('Tell me about '+current.title);assert.equal((await h.tool('inspect_jewellery',{handle:current.handle},inspection)).live,true);const count=h.calls.reads.length,context=h.speech(action==='review'?'Review adding Moon earrings 20 in sterling silver to my bag.':action==='options'?'Show options for Moon earrings 20.':'Open Moon earrings 20.'),checked=await h.tool('prepare_jewellery_action',{handle:current.handle,action,...(action==='review'?{variantId:current.variants[0].id}:{})},context);assert.equal(checked.ok,false,'The actual host callback refuses unsupported preparation');assert(checked.reply.trim(),'The refusal contains customer copy');assert.doesNotMatch(checked.reply,/backend|postcondition|completedActions|NO_ACTION_AUTHORITY|no further action|connected\s+storefront/i);assert.equal(checked.prepared,undefined,JSON.stringify(checked));assert.equal(h.calls.reads.length,count);assert.equal(h.root.querySelectorAll('.card,select,.review,.prepared-view').length,0);assert.equal(h.saved().products.length,0);assert.equal(h.w.sessionStorage.getItem('brites-sandbox-cart'),null);assert.deepEqual(h.calls.navigations,[]);
});

for(const message of ['Could you highlight their meaning?','Would you scroll to the symbolism of each?','Please open the meaning of both earrings.','Will you highlight the story of them?'])test('an explicit unresolved plural page control stays recognized and cannot acquire an invented target: '+message,()=>{
  const result=Bridge.resolve(message,{pageKind:'collection',contextRevision:2,discoveryRevision:2,loading:false,currentHandle:'',focusedHandle:'',visiblePieces:visible.map(({id,handle,title})=>({id,handle,title}))});assert.equal(result.recognized,true);assert.equal(result.ok,false);assert.equal(result.action,undefined);
});
