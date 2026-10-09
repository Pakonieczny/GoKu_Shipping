'use strict';
// Synthetic DOM integration only. These checks do not certify a live realtime
// provider, actual microphone permissions, voice quality or GPU appearance.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const {JSDOM, VirtualConsole} = require('jsdom');
const source = fs.readFileSync(require.resolve('../../brites-concierge.js'), 'utf8');
const tick = () => new Promise(resolve => setImmediate(resolve));
async function settle() {await tick(); await tick();}
const variant = {id:'gid://shopify/ProductVariant/101',numericId:'101',title:'Sterling Silver',price:54,available:true,options:[{name:'Metal',value:'Sterling Silver'}]};
const product = {id:'gid://shopify/Product/1',handle:'bunny-1',url:'https://britesjewelry.com/products/bunny-1',title:'Bunny Necklace',type:'Necklace',currency:'USD',variants:[variant],variantsComplete:true,suggestedVariantId:variant.id,minPrice:54,why:'A personal symbol for your milestone.'};
function response(body) {return {ok:true,status:200,json:async()=>JSON.parse(JSON.stringify(body))};}
function harness(t, options = {}) {
  const errors = [], console = new VirtualConsole(); console.on('jsdomError', error => errors.push(error));
  const dom = new JSDOM('<!doctype html><html lang="en"><body><button id="ordinary">Shop</button></body></html>', {url:'https://growth-sandbox.example/concierge-sandbox.html',runScripts:'outside-only',pretendToBeVisual:true,virtualConsole:console});
  const win = dom.window, document = win.document; let hidden = false;
  Object.defineProperty(document, 'hidden', {get:()=>hidden});
  const current = document.createElement('script'); current.src = 'https://growth-sandbox.example/brites-concierge.js'; current.dataset.sandbox = 'true';
  Object.defineProperty(document, 'currentScript', {get:()=>current});
  const deviceCalls = [], voiceCalls = {create:0,start:0,stop:0,dispose:0,interrupt:0}, avatarCalls = {create:0,states:[],emotions:[],paused:[],levels:[],visible:[],greetings:0,options:[]}, network = [];
  win.navigator.mediaDevices = {getUserMedia:async() => {deviceCalls.push('microphone');return {};},enumerateDevices:async() => {deviceCalls.push('enumerate');return [];}};
  win.BritesConciergeAvatar = {create:options => {avatarCalls.create++; avatarCalls.options.push(options); return {setState:value=>avatarCalls.states.push(value),setEmotion:value=>avatarCalls.emotions.push(value),setPaused:value=>avatarCalls.paused.push(value),setLevel:value=>avatarCalls.levels.push(value),setVisible:value=>avatarCalls.visible.push(value),retry(){},destroy(){},triggerGreeting(){avatarCalls.greetings++;return true;}};}};
  let voiceConfig;
  win.BritesConciergeVoice = {create:config => {voiceCalls.create++;if(options.createThrows)throw Error('Synthetic voice constructor unavailable');voiceConfig=config;return {
    start:async()=>{voiceCalls.start++;if(options.startThrows)throw Error('Disabled realtime provider');if(options.startMic)await win.navigator.mediaDevices.getUserMedia({audio:true});return options.started===true;},
    stop:async()=>{voiceCalls.stop++;},dispose:async()=>{voiceCalls.dispose++;},interrupt(){voiceCalls.interrupt++;}
  };}};
  win.fetch = async (raw, init = {}) => {const url = new URL(raw,win.location.href), body = init.body ? JSON.parse(init.body) : null;network.push({url,body});
    if(url.pathname==='/api/concierge'&&body?.event)return response({});
    if(url.pathname==='/api/concierge'&&body?.message)return response(options.answer || {live:true,reply:'This might hold a personal meaning for you.',preferences:{},products:[product],meanings:[]});
    if(url.pathname==='/api/growth/product')return response({product});
    throw Error('Unexpected provider or cart request: '+url.pathname);
  };
  win.HTMLElement.prototype.scrollIntoView = function(){};
  win.eval(source);
  const root = document.querySelector('brites-concierge').shadowRoot;
  const button = text => Array.from(root.querySelectorAll('button')).find(node=>node.textContent.trim()===text||node.getAttribute('aria-label')===text);
  const voiceButton = button('Talk to me');
  t.after(()=>{try{win.BritesConcierge.close();}catch{}win.close();});
  return {win,document,root,button,voiceButton,deviceCalls,voiceCalls,avatarCalls,network,errors,
    open:()=>win.BritesConcierge.open(),close:()=>win.BritesConcierge.close(),
    hide:value=>{hidden=value;document.dispatchEvent(new win.Event('visibilitychange'));},
    get voiceConfig(){return voiceConfig;},
    loader:()=>root.querySelector('script[src$="brites-concierge-voice.js"]'),
    async activate(){voiceButton.click();await settle();const loader=root.querySelector('script[src$="brites-concierge-voice.js"]');if(loader)loader.dispatchEvent(new win.Event('load'));await settle();},
    async ask(text){const input=root.querySelector('input[aria-label="Message the gift concierge"]');input.value=text;root.querySelector('form').dispatchEvent(new win.Event('submit',{bubbles:true,cancelable:true}));await settle();}
  };
}
test('load and opening remain silent: no voice adapter, microphone or provider request before explicit opt-in', async t => {
  const h=harness(t,{started:true,startMic:true}); await settle();
  assert.equal(h.loader(),null);assert.equal(h.voiceCalls.create,0);assert.equal(h.voiceCalls.start,0);assert.deepEqual(h.deviceCalls,[]);assert.equal(h.network.length,0);
  h.open();await settle();assert.equal(h.loader(),null);assert.equal(h.voiceCalls.create,0);assert.deepEqual(h.deviceCalls,[]);
  assert.ok(h.network.every(call=>call.url.pathname==='/api/concierge'&&call.body.event));
  assert.equal(h.voiceButton.getAttribute('aria-pressed'),'false');
});
test('explicit Talk loads the adapter once; feature-disabled start returns an inactive usable control', async t => {
  const h=harness(t,{started:false});h.open();h.voiceButton.click();await settle();
  assert.ok(h.loader());assert.equal(h.voiceButton.disabled,false);assert.equal(h.voiceButton.textContent,'Cancel connection');assert.equal(h.voiceButton.getAttribute('aria-pressed'),'true');assert.equal(h.voiceCalls.start,0);
  h.loader().dispatchEvent(new h.win.Event('load'));await settle();
  assert.equal(h.voiceCalls.create,1);assert.equal(h.voiceCalls.start,1);assert.equal(h.voiceButton.disabled,false);assert.equal(h.voiceButton.textContent,'Talk to me');assert.equal(h.voiceButton.getAttribute('aria-pressed'),'false');
  assert.equal(h.root.querySelector('.voice-state').dataset.active,'false');assert.deepEqual(h.deviceCalls,[]);
  await h.activate();assert.equal(h.voiceCalls.create,1);assert.equal(h.voiceCalls.start,2);assert.equal(h.loader(),null);
});
test('adapter script failure and start rejection restore inactive controls while typed shopping remains usable', async t => {
  const h=harness(t,{startThrows:true});h.open();h.voiceButton.click();await settle();h.loader().dispatchEvent(new h.win.Event('error'));await settle();
  assert.equal(h.voiceButton.disabled,false);assert.equal(h.voiceCalls.start,0);assert.equal(h.voiceButton.getAttribute('aria-pressed'),'false');
  assert.equal(h.root.querySelector('.voice-state').textContent,'Voice not connected','failed adapter load must retain a truthful connection result');
  await h.activate();assert.equal(h.voiceButton.disabled,false);assert.equal(h.voiceButton.getAttribute('aria-pressed'),'false');
  assert.match(h.root.querySelector('.status').textContent,/Voice is unavailable/);
  await h.ask('A bunny necklace');assert.equal(h.root.querySelectorAll('.card').length,1);assert.deepEqual(h.deviceCalls,[]);
});
test('successful explicit voice opt-in toggles off and dismissal/hidden document stop the adapter', async t => {
  const h=harness(t,{started:true,startMic:true});h.open();await h.activate();
  assert.equal(h.voiceButton.textContent,'End voice');assert.equal(h.voiceButton.getAttribute('aria-pressed'),'true');assert.deepEqual(h.deviceCalls,['microphone']);
  h.voiceConfig.onState('listening');assert.match(h.root.querySelector('.voice-state').textContent,/Listening/);
  h.voiceConfig.onLevel({input:.2,output:.6});assert.equal(h.avatarCalls.levels.at(-1),0,'listening does not animate output speech');h.voiceConfig.onState('speaking');h.voiceConfig.onLevel({input:.2,output:.6});assert.equal(h.avatarCalls.levels.at(-1),.6);
  const stops=h.voiceCalls.stop;h.voiceButton.click();await settle();assert.ok(h.voiceCalls.stop>stops);assert.equal(h.voiceButton.getAttribute('aria-pressed'),'false');
  await h.activate();const hiddenStops=h.voiceCalls.stop;h.hide(true);await settle();assert.ok(h.voiceCalls.stop>hiddenStops);assert.equal(h.voiceButton.getAttribute('aria-pressed'),'false');assert.equal(h.avatarCalls.visible.at(-1),false);
  h.hide(false);await h.activate();const closeStops=h.voiceCalls.stop;h.close();await settle();assert.ok(h.voiceCalls.stop>closeStops);assert.equal(h.voiceButton.getAttribute('aria-pressed'),'false');
  const last=h.avatarCalls.states.length;h.voiceConfig.onState('speaking');assert.equal(h.avatarCalls.states.length,last,'dismissed conversation ignores late state callbacks');
});
test('Pause and Resume controls explicitly pause avatar animation without starting voice', async t => {
  const h=harness(t);h.open();await settle();assert.equal(h.avatarCalls.paused.at(-1),false);
  const control=h.button('Pause animation');control.click();assert.equal(h.avatarCalls.paused.at(-1),true);assert.equal(control.getAttribute('aria-pressed'),'true');assert.equal(control.textContent,'Resume animation');
  control.click();assert.equal(h.avatarCalls.paused.at(-1),false);assert.equal(control.getAttribute('aria-pressed'),'false');assert.equal(control.textContent,'Pause animation');assert.equal(h.voiceCalls.create,0);assert.deepEqual(h.deviceCalls,[]);
});
test('bereavement conversation stays calm and avoids celebration even when products are found', async t => {
  const h=harness(t);h.open();await settle();h.avatarCalls.states.length=0;
  await h.ask('My mother passed away. I want a memorial necklace.');
  assert.equal(h.avatarCalls.emotions.at(-1),'calm');assert.equal(h.avatarCalls.states.at(-1),'idle');assert.ok(!h.avatarCalls.states.includes('success'));assert.equal(h.root.querySelectorAll('.card').length,1);
  const birthdayStart=h.avatarCalls.emotions.length;await h.ask('Another gift: A birthday gift with a bunny');const birthday=h.avatarCalls.emotions.slice(birthdayStart);assert.ok(birthday.includes('celebrate'),'the explicitly independent birthday gift is acknowledged');assert.equal(birthday.at(-1),'warm','the neutral assistant reply supplies the following tone');assert.equal(h.avatarCalls.states.at(-1),'success');
});
test('final voice transcripts update contextual emotion and conversation but do not issue a catalogue or cart request by themselves', async t => {
  const h=harness(t,{started:true});h.open();await h.activate();const before=h.network.length;
  h.voiceConfig.onTranscript({role:'user',text:'A necklace in memory of my father who died',final:true});
  assert.equal(h.avatarCalls.emotions.at(-1),'calm');assert.match(h.root.querySelector('.messages').textContent,/memory of my father/);assert.equal(h.network.length,before);
  h.voiceConfig.onTranscript({role:'assistant',text:'I can help you find a personal symbol.',final:false});
  assert.doesNotMatch(h.root.querySelector('.messages').textContent,/I can help you find a personal symbol/);
});
for(const requestedType of ['navigate','choose','add'])test('voice catalogue tool returns a public whitelist and cannot automatically '+requestedType,async t=>{
  const h=harness(t,{started:true,answer:{live:true,reply:'A bunny may suggest a new beginning.',question:'Which metal?',preferences:{budget:60},products:[{...product,accountId:'private-account',rank:7,credentials:'private-key',internalInstructions:'private instructions'}],meanings:[{productId:product.id,text:'A personal new beginning.',context:'Your interpretation',sources:[{title:'Museum symbol reference',url:'https://www.metmuseum.org/',credentials:'private-key',internalInstructions:'private instructions'}],privateRank:4}],requestedAction:{type:requestedType,productId:product.id,url:product.url},accountId:'private-account',internalInstructions:'private instructions'}});
  h.open();await h.activate();const before=h.win.location.href;
  const result=await h.voiceConfig.onTool({message:'Please '+requestedType+' the bunny'});
  assert.deepEqual(Object.keys(result).sort(),['actions','displayedPieces','meanings','products','question','reply','verified']);assert.equal(result.verified,true);assert.equal(JSON.stringify(result.actions),'[]');
  assert.deepEqual(Object.keys(result.products[0]).sort(),['cartHold','currency','handle','id','minPrice','partsOnly','recommendationHold','title','type','url','variantsComplete','why']);
  assert.deepEqual(Object.keys(result.meanings[0]).sort(),['context','productId','sources','text']);assert.deepEqual(Object.keys(result.meanings[0].sources[0]).sort(),['title','url']);assert.doesNotMatch(JSON.stringify(result),/private-account|private-key|private instructions|privateRank/);
  assert.equal(h.win.location.href,before);assert.equal(h.root.querySelector('.options'),null);assert.equal(h.root.querySelector('.review'),null);assert.equal(h.win.sessionStorage.getItem('brites-sandbox-cart'),null);
  assert.ok(h.network.every(call=>call.url.pathname==='/api/concierge'));assert.ok(!h.errors.some(error=>/navigation/i.test(error.message)));
});
test('dismissed or malformed voice tool calls return a safe error without checking products',async t=>{
  const h=harness(t,{started:true});h.open();await h.activate();const before=h.network.filter(call=>call.body?.message).length;
  assert.match((await h.voiceConfig.onTool({message:null})).error,/busy/);h.close();assert.match((await h.voiceConfig.onTool({message:'Add a necklace'})).error,/busy/);
  assert.equal(h.network.filter(call=>call.body?.message).length,before);
});

test('voice constructor failure cannot strand the Talk control in a disabled loading state',async t=>{
  const h=harness(t,{createThrows:true});h.open();await h.activate();
  assert.equal(h.voiceButton.disabled,false);assert.equal(h.voiceButton.getAttribute('aria-pressed'),'false');
  assert.equal(h.root.querySelector('.voice-state').textContent,'Voice not connected');
  await h.ask('A bunny necklace');assert.equal(h.root.querySelectorAll('.card').length,1);
});

test('the optional Meet invitation stays closed and silent until explicitly opened', async t => {
  const h=harness(t);const launch=h.root.querySelector('.launcher');
  assert.match(launch.textContent,/Meet your gift guide/);assert.equal(launch.getAttribute('aria-expanded'),'false');
  assert.equal(h.root.querySelector('.panel').hidden,true);assert.equal(h.avatarCalls.create,0);assert.equal(h.voiceCalls.create,0);assert.deepEqual(h.deviceCalls,[]);
  launch.click();await settle();assert.equal(launch.getAttribute('aria-expanded'),'true');assert.equal(h.avatarCalls.create,1);assert.equal(h.voiceCalls.create,0);
});
test('the milestone invitation routes a real contextual request without starting voice', async t => {
  const h=harness(t);h.open();h.button('Celebrate a milestone').click();await settle();
  assert.equal(h.network.find(call=>call.body?.message)?.body.message,'I would like a meaningful piece to celebrate an achievement.');assert.equal(h.voiceCalls.create,0);assert.deepEqual(h.deviceCalls,[]);
});

test('the budget invitation says that its inclusive sixty-dollar cap includes exactly sixty dollars', async t => {
  const h=harness(t);h.open();h.button('Help me choose for $60 or less').click();await settle();
  assert.equal(h.network.find(call=>call.body?.message)?.body.message,'Help me choose for $60 or less');
});
test('the remembrance invitation uses a calm pose instead of a celebration', async t => {
  const h=harness(t,{answer:{reply:'Choose a personal way to mark that memory.',preferences:{milestone:'remembrance'},products:[product],meanings:[]}});
  h.open();h.button('Remember someone').click();await settle();
  assert.equal(h.network.find(call=>call.body?.message)?.body.message,'I would like a meaningful remembrance piece.');assert.equal(h.avatarCalls.emotions.at(-1),'calm');
});
test('a rejected memorial occasion does not suppress a corrected birthday celebration', async t => {
  const h=harness(t);h.open();await settle();const before=h.avatarCalls.emotions.length;await h.ask('Not a memorial gift. A bunny birthday gift instead.');const correction=h.avatarCalls.emotions.slice(before);assert.ok(correction.includes('celebrate'),'the stated birthday receives its celebration');assert.equal(correction.includes('calm'),false,'a negated memorial does not impose a grief presentation');assert.equal(correction.at(-1),'warm','the ordinary assistant explanation follows the birthday acknowledgement');
});


test('the gesture greeting follows an explicit opening once and is never replayed by redundant open calls', async t => {
  const h=harness(t);h.open();await settle();assert.equal(h.avatarCalls.options[0].greetingOnOpen,false);assert.equal(h.avatarCalls.greetings,1);
  h.open();await settle();assert.equal(h.avatarCalls.greetings,1);h.close();h.open();await settle();assert.equal(h.avatarCalls.greetings,1);
});
