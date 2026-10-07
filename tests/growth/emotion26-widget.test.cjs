'use strict';
// Exercise the actual widget with synthetic model/catalogue replies and an
// instrumented avatar. These tests do not certify native audio or GPU output.
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs');
const {JSDOM,VirtualConsole}=require('jsdom');
const source=fs.readFileSync(require.resolve('../../brites-concierge.js'),'utf8');
const clone=x=>JSON.parse(JSON.stringify(x)),tick=()=>new Promise(r=>setImmediate(r));
async function settle(){await tick();await tick();await tick();}
function deferred(){let resolve;return {promise:new Promise(r=>{resolve=r;}),resolve:v=>resolve(v)};}
const variant={id:'gid://shopify/ProductVariant/26101',numericId:'26101',title:'Sterling Silver / 16 inch / None',price:54,available:true,options:[{name:'Metal',value:'Sterling Silver'},{name:'Length',value:'16 inch'},{name:'Engraving',value:'None'}]};
const piece={id:'gid://shopify/Product/261',handle:'compass-necklace',url:'https://britesjewelry.com/products/compass-necklace',title:'Compass Necklace',type:'Necklace',currency:'USD',variants:[variant],variantsComplete:true,minPrice:54,suggestedVariantId:variant.id,why:'A personal milestone reminder.'};
const shopping={live:true,reply:'Here is the checked piece.',preferences:{},products:[piece],meanings:[]};
const general={live:false,conversationOnly:true,preserveSelection:true,conversationKind:'general',needsModelConversation:true,reply:'I can chat while you browse.',preferences:{},products:[],meanings:[]};
const performance={mood:'curious',gesture:'acknowledge',intensity:.35,durationMs:1100};
const response=value=>({ok:true,json:async()=>clone(value)});
function fixture(t,options={}){
  const vc=new VirtualConsole(),errors=[];vc.on('jsdomError',e=>errors.push(e));
  const dom=new JSDOM('<!doctype html><body><a id="page-piece" href="/concierge-sandbox.html?product=compass-necklace">Compass</a></body>',{url:'https://growth-sandbox.example/concierge-sandbox.html',runScripts:'outside-only',pretendToBeVisual:true,virtualConsole:vc});
  const w=dom.window,d=w.document,script=d.createElement('script');script.src='https://growth-sandbox.example/brites-concierge.js';script.dataset.sandbox='true';Object.defineProperty(d,'currentScript',{get:()=>script});
  let hidden=false;Object.defineProperty(d,'hidden',{get:()=>hidden});
  const calls=[],avatar={performances:[],cancels:[],emotions:[],states:[],focus:[],sequence:[]};
  let config,modelCount=0;
  w.BritesConciergeAvatar={create:()=>({setState:v=>{avatar.states.push(v);avatar.sequence.push(['state',v]);},setEmotion:v=>avatar.emotions.push(v),setVisible(){},setPaused:v=>avatar.sequence.push(['paused',v]),setLevel(){},retry(){},triggerGreeting(){},cue(){},focusProduct:v=>avatar.focus.push(clone(v)),clearFocus(){},perform:v=>{avatar.performances.push(clone(v));avatar.sequence.push(['perform',clone(v)]);return true;},cancelPerformance:()=>{avatar.cancels.push(true);avatar.sequence.push(['cancel']);}})};
  const voice={start:async()=>true,stop:async()=>{},dispose:async()=>{},interrupt(){},updateContext(){}};
  w.BritesConciergeVoice={create:value=>{config=value;return voice;}};w.HTMLElement.prototype.scrollIntoView=function(){};
  w.fetch=async(raw,init={})=>{const url=new URL(raw,w.location.href),body=init.body?JSON.parse(init.body):null;const call={url,body,signal:init.signal};calls.push(call);
    if(url.pathname==='/api/concierge'&&body?.event)return response({});
    if(url.pathname==='/api/concierge'&&body?.message)return response(options.fast?options.fast(body):body.message==='Find a compass necklace'?shopping:general);
    if(url.pathname==='/api/concierge-demo-turn'){modelCount++;return options.model?options.model(call,modelCount):response({conversationOnly:true,reply:'That is an interesting way to think about it.',aiUsed:true,avatarPerformance:clone(performance)});}
    if(url.pathname==='/api/growth/product')return options.readProduct?options.readProduct(call):response({live:true,product:piece});
    throw Error('Unexpected route '+url.pathname);
  };
  w.eval(source);const root=d.querySelector('brites-concierge').shadowRoot,button=label=>[...root.querySelectorAll('button')].find(b=>b.textContent.trim()===label||b.getAttribute('aria-label')===label);
  t.after(()=>{try{w.BritesConcierge.close();}catch{}w.close();});
  let inputVersion=0;
  return {w,d,root,calls,avatar,errors,button,get config(){return config;},async open(){w.BritesConcierge.open();await settle();},async ask(message){const input=root.querySelector('input[aria-label="Message the gift concierge"]');input.value=message;root.querySelector('form').dispatchEvent(new w.Event('submit',{bubbles:true,cancelable:true}));await settle();},async voice(){button('Talk to me').click();await settle();root.querySelector('script[src$="brites-concierge-voice.js"]')?.dispatchEvent(new w.Event('load'));await settle();},nativeInput(message='I like the idea of a thoughtful gift.'){const turnVersion=++inputVersion,itemId='input-'+turnVersion;config.onSpeechStarted({itemId,turnVersion});config.onTranscript({role:'user',itemId,turnVersion,currentTurn:true,text:message,final:true});return {responseId:'response-'+turnVersion,inputItemId:itemId,turnVersion,currentTurn:true};},hide(value){hidden=value;d.dispatchEvent(new w.Event('visibilitychange'));},reset(){avatar.performances.length=avatar.cancels.length=avatar.emotions.length=avatar.states.length=avatar.sequence.length=0;}};
}

test('a current typed model reply performs only its validated expressive envelope',async t=>{
  const h=fixture(t);await h.open();h.reset();await h.ask('What makes a keepsake interesting?');assert.deepEqual(h.avatar.performances,[performance]);assert.equal(h.w.sessionStorage.getItem('brites-sandbox-cart'),null);assert.equal(h.root.querySelector('.review'),null);assert.equal(h.w.location.pathname,'/concierge-sandbox.html');
});

for(const [name,value] of [
  ['unknown mood',{...performance,mood:'buy-now'}],['unknown gesture',{...performance,gesture:'checkout'}],['negative intensity',{...performance,intensity:-1}],['unbounded intensity',{...performance,intensity:2}],['nonfinite intensity',{...performance,intensity:null}],['short duration',{...performance,durationMs:399}],['long duration',{...performance,durationMs:2501}],['fractional duration',{...performance,durationMs:1100.5}],['URL field',{...performance,url:piece.url}],['action field',{...performance,action:'add-to-cart'}],['missing mood',{gesture:'greet',intensity:.4,durationMs:1000}],['string payload','greet'],['array payload',[performance]]
])test('malformed model performance '+name+' cannot reach the avatar or authorize a shop action',async t=>{
  const h=fixture(t,{model:()=>response({conversationOnly:true,reply:'A safe ordinary reply.',aiUsed:true,avatarPerformance:value,requestedAction:{type:'navigate',url:piece.url},products:[piece]})});await h.open();h.reset();await h.ask('Tell me something interesting');assert.deepEqual(h.avatar.performances,[]);assert.equal(h.w.sessionStorage.getItem('brites-sandbox-cart'),null);assert.equal(h.w.location.pathname,'/concierge-sandbox.html');assert.equal(h.root.querySelector('.review'),null);
});

test('a new accepted message cancels the prior performance before applying its fresh response',async t=>{
  const h=fixture(t);await h.open();await h.ask('What makes a gift memorable?');h.reset();await h.ask('How do symbols get their meaning?');const cancelled=h.avatar.sequence.findIndex(v=>v[0]==='cancel'),performed=h.avatar.sequence.findIndex(v=>v[0]==='perform');assert.ok(cancelled>=0&&performed>cancelled);assert.deepEqual(h.avatar.performances,[performance]);
});

for(const boundary of ['hidden','dismiss','fresh','pause'])test(boundary+' cancels the active performance and blocks a late model cue',async t=>{
  const pending=deferred(),h=fixture(t,{model:()=>pending.promise});await h.open();h.reset();await h.ask('Can you explain why people like symbols?');const call=h.calls.find(c=>c.url.pathname==='/api/concierge-demo-turn');assert.ok(call);const before=h.avatar.cancels.length;
  if(boundary==='hidden')h.hide(true);else if(boundary==='dismiss')h.w.BritesConcierge.close();else if(boundary==='fresh')h.button('Start fresh').click();else h.button('Pause animation').click();
  assert.ok(h.avatar.cancels.length>before,'Boundary must explicitly cancel the active expressive envelope');pending.resolve(response({conversationOnly:true,reply:'The delayed reply.',aiUsed:true,avatarPerformance:performance}));await settle();assert.deepEqual(h.avatar.performances,[]);assert.equal(h.w.sessionStorage.getItem('brites-sandbox-cart'),null);
});

test('a restarted conversation ignores an old model performance after the newer reply arrives',async t=>{
  const old=deferred(),latest={...performance,mood:'warm',gesture:'reassure'},h=fixture(t,{model:(call,index)=>index===1?old.promise:response({conversationOnly:true,reply:'The current reply.',aiUsed:true,avatarPerformance:latest})});await h.open();await h.ask('Tell me about meaningful objects');h.button('Start fresh').click();await settle();h.reset();await h.ask('Let us try a new thought');old.resolve(response({conversationOnly:true,reply:'Old cancelled reply.',aiUsed:true,avatarPerformance:performance}));await settle();assert.deepEqual(h.avatar.performances,[latest]);assert.doesNotMatch(h.root.textContent,/Old cancelled reply/);
});

test('gratitude may be appreciated but frustration wins over gratitude in a mixed feedback turn',async t=>{
  const social={...general,conversationKind:'thanks',needsModelConversation:false,reply:'You are welcome.'},h=fixture(t,{fast:()=>social});await h.open();h.reset();await h.ask('Thank you 🙏');assert.ok(h.avatar.emotions.includes('appreciated'));h.reset();await h.ask('Thanks, but this is not helpful and I am frustrated.');assert.ok(h.avatar.emotions.includes('reassuring'));assert.equal(h.avatar.emotions.includes('appreciated'),false);
});

test('hover attention moves toward a product without starting model conversation, navigation or a bag action',async t=>{
  const h=fixture(t);await h.open();await h.ask('Find a compass necklace');h.reset();const before=h.calls.length,href=h.w.location.href,card=h.root.querySelector('.card');card.dispatchEvent(new h.w.Event('pointerenter'));h.d.querySelector('#page-piece').dispatchEvent(new h.w.Event('pointerover',{bubbles:true}));await settle();assert.ok(h.avatar.focus.length>0);assert.equal(h.calls.length,before);assert.equal(h.w.location.href,href);assert.equal(h.w.sessionStorage.getItem('brites-sandbox-cart'),null);assert.deepEqual(h.avatar.performances,[]);
});

test('native expressive cues wait for speaking while connected and are ignored after End voice',async t=>{
  const h=fixture(t);await h.open();await h.voice();assert.equal(typeof h.config.onAvatarPerformance,'function');const metadata=h.nativeInput();h.config.onState('thinking');h.reset();h.config.onAvatarPerformance(performance,metadata);assert.deepEqual(h.avatar.performances,[]);h.config.onPlaybackState({...metadata,itemId:'spoken-output',playing:true,cleared:false});assert.deepEqual(h.avatar.performances,[]);h.config.onState('speaking');assert.deepEqual(h.avatar.performances,[performance]);h.button('End voice').click();await settle();h.reset();h.config.onAvatarPerformance({...performance,mood:'celebrate'},metadata);assert.deepEqual(h.avatar.performances,[]);
});

for(const boundary of ['speech','interrupt','state','cancel-callback'])test('native '+boundary+' cancels an expressive cue without performing a shop action',async t=>{
  const h=fixture(t);await h.open();await h.voice();const metadata=h.nativeInput();h.config.onPlaybackState({...metadata,itemId:'spoken-output',playing:true,cleared:false});h.config.onState('speaking');h.config.onAvatarPerformance(performance,metadata);assert.deepEqual(h.avatar.performances,[performance]);h.reset();
  if(boundary==='speech')h.config.onSpeechStarted({itemId:'new-input',turnVersion:2});
  else if(boundary==='interrupt'){h.config.onState('speaking');h.button('Let me speak').click();}
  else if(boundary==='state')h.config.onState('listening');
  else {assert.equal(typeof h.config.onAvatarPerformanceCancelled,'function');h.config.onAvatarPerformanceCancelled({reason:'speech',turnVersion:2});}
  assert.ok(h.avatar.cancels.length>0);assert.deepEqual(h.avatar.performances,[]);assert.equal(h.w.sessionStorage.getItem('brites-sandbox-cart'),null);
});

for(const boundary of ['speech','interrupt','state','cancel-callback'])test('native '+boundary+' discards a queued cue before a later speaking transition',async t=>{
  const h=fixture(t);await h.open();await h.voice();const metadata=h.nativeInput();h.config.onState('thinking');h.reset();h.config.onAvatarPerformance(performance,metadata);assert.deepEqual(h.avatar.performances,[]);
  if(boundary==='speech')h.config.onSpeechStarted({itemId:'new-input',turnVersion:2});else if(boundary==='interrupt'){h.config.onState('speaking');h.button('Let me speak').click();h.reset();}else if(boundary==='state')h.config.onState('listening');else h.config.onAvatarPerformanceCancelled({reason:'speech',turnVersion:2});
  h.config.onState('speaking');assert.deepEqual(h.avatar.performances,[]);assert.equal(h.w.sessionStorage.getItem('brites-sandbox-cart'),null);
});

test('a general reply without actual AI use cannot install model-controlled expression',async t=>{
  const h=fixture(t,{model:()=>response({conversationOnly:true,reply:'A safe fallback reply.',aiUsed:false,avatarPerformance:performance})});await h.open();h.reset();await h.ask('Tell me something interesting');assert.deepEqual(h.avatar.performances,[]);
});

test('pending and recently verified bag actions take priority over model expression and report real progress',async t=>{
  const pending=deferred(),h=fixture(t,{readProduct:()=>pending.promise});await h.open();await h.ask('Find a compass necklace');await h.voice();h.button('Choose options').click();assert.equal(h.config.getContext().progress,'options-shown');h.button('Review adding to bag').click();assert.equal(h.config.getContext().progress,'review-ready');h.button('Confirm add to bag').click();await settle();const metadata=h.nativeInput();h.reset();h.config.onAvatarPerformance({...performance,mood:'celebrate'},metadata);assert.deepEqual(h.avatar.performances,[]);assert.equal(h.w.sessionStorage.getItem('brites-sandbox-cart'),null,'An in-flight catalogue read is not a confirmed bag change');pending.resolve(response({live:true,product:piece}));await settle();assert.equal(JSON.parse(h.w.sessionStorage.getItem('brites-sandbox-cart')).length,1);assert.equal(h.config.getContext().progress,'cart-confirmed');h.reset();h.config.onAvatarPerformance(performance,metadata);assert.deepEqual(h.avatar.performances,[],'The actual success beat must not be overwritten by a generic delayed cue');
});

for(const [name,mutate] of [['wrong input',m=>({...m,inputItemId:'other-input'})],['wrong turn',m=>({...m,turnVersion:m.turnVersion+1})],['not current',m=>({...m,currentTurn:false})],['missing metadata',()=>undefined]])test('widget rejects a native expression with '+name+' attribution',async t=>{
  const h=fixture(t);await h.open();await h.voice();const metadata=h.nativeInput();h.config.onPlaybackState({...metadata,itemId:'spoken-output',playing:true,cleared:false});h.config.onState('speaking');h.reset();h.config.onAvatarPerformance(performance,mutate(metadata));assert.deepEqual(h.avatar.performances,[]);assert.equal(h.w.sessionStorage.getItem('brites-sandbox-cart'),null);
});

for(const [name,message] of [['bereavement','My mother passed away and I am grieving.'],['frustration','Thanks, but I am frustrated and this is not helpful.']])test(name+' context prevents an overshooting model celebration or heart expression',async t=>{
  const overshoot={mood:'appreciated',gesture:'confirm',intensity:1,durationMs:2500},h=fixture(t,{model:()=>response({conversationOnly:true,reply:'We can take this at your pace.',aiUsed:true,avatarPerformance:overshoot})});await h.open();h.reset();await h.ask(message);assert.equal(h.avatar.performances.length,1);assert.ok(['calm','reassuring'].includes(h.avatar.performances[0].mood));assert.notEqual(h.avatar.performances[0].gesture,'confirm');assert.equal(h.w.sessionStorage.getItem('brites-sandbox-cart'),null);
});
