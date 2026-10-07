'use strict';
// Actual widget callbacks with synthetic audio energy. This is a UI contract,
// not a claim that physical microphone/audio or GPU rendering was tested.
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs');
const {JSDOM,VirtualConsole}=require('jsdom');
const source=fs.readFileSync(require.resolve('../../brites-concierge.js'),'utf8');
const tick=()=>new Promise(resolve=>setImmediate(resolve));
async function settle(){await tick();await tick();}
function fixture(t){
  const vc=new VirtualConsole(),errors=[];vc.on('jsdomError',error=>errors.push(error));
  const dom=new JSDOM('<!doctype html><body></body>',{url:'https://growth-sandbox.example/concierge-sandbox.html',runScripts:'outside-only',pretendToBeVisual:true,virtualConsole:vc});
  const w=dom.window,d=w.document,script=d.createElement('script');script.src='https://growth-sandbox.example/brites-concierge.js';script.dataset.sandbox='true';Object.defineProperty(d,'currentScript',{get:()=>script});
  const calls={levels:[],emotions:[],states:[],starts:0,stops:0,resumes:0,requests:[],performances:[],timers:[]};let config,clock=Date.now();w.Date.now=()=>clock;const nativeSet=w.setTimeout.bind(w),nativeClear=w.clearTimeout.bind(w);w.setTimeout=(fn,ms)=>{if(ms===1250){const token=100000+calls.timers.length;calls.timers.push({fn,token,ms});return token;}return nativeSet(fn,ms);};w.clearTimeout=token=>{const timer=calls.timers.find(value=>value.token===token);if(timer)timer.cancelled=true;else nativeClear(token);};
  w.BritesConciergeAvatar={create:()=>({setState:value=>calls.states.push(value),setLevel:value=>calls.levels.push(value),setEmotion:value=>calls.emotions.push(value),setPaused(){},setVisible(){},triggerGreeting(){},retry(){},perform:value=>{calls.performances.push(value);return true;},cancelPerformance(){}})};
  const client={state:'listening',outputMeterState:'waiting',playbackBlocked:false,start:async()=>{calls.starts++;return true;},stop:async()=>{calls.stops++;},interrupt(){},dispose:async()=>{},resumeAudio:async()=>{calls.resumes++;client.playbackBlocked=false;return true;}};
  w.BritesConciergeVoice={create:value=>{config=value;return client;}};
  w.fetch=async(raw,init={})=>{calls.requests.push({url:String(raw),body:init.body?JSON.parse(init.body):null});return {ok:true,json:async()=>({})};};
  w.eval(source);const root=d.querySelector('brites-concierge').shadowRoot,button=label=>[...root.querySelectorAll('button')].find(value=>value.textContent===label);
  t.after(()=>{try{w.BritesConcierge.close();}catch{}w.close();});
  return {w,root,calls,errors,client,button,advance:ms=>{clock+=ms;},get config(){return config;},async start(){w.BritesConcierge.open();button('Talk to me').click();await settle();root.querySelector('script[src$="brites-concierge-voice.js"]')?.dispatchEvent(new w.Event('load'));await settle();},state(value){client.state=value;config.onState(value);},meter(value){client.outputMeterState=value;config.onOutputMeterState(value);},speak(text,id='input-1',version=1){config.onSpeechStarted({itemId:id,turnVersion:version});config.onTranscript({role:'user',itemId:id,turnVersion:version,currentTurn:true,text,final:true});}};
}

test('generated outgoing words preserve the conversation baseline while only real remote energy drives speech',async t=>{
  const h=fixture(t);await h.start();h.speak('A gift for my graduation.');h.state('speaking');h.meter('ready');
  h.config.onTranscript({role:'assistant',itemId:'reply-1',delta:'Congratulations on your graduation.',final:false});assert.equal(h.calls.emotions.at(-1),'celebrate');
  h.config.onLevel({input:.95,output:.21});assert.equal(h.calls.levels.at(-1),.21);
  h.config.onTranscript({role:'assistant',itemId:'reply-1',delta:' Which design feels most personal to you?',final:false});assert.equal(h.calls.emotions.at(-1),'celebrate','generation must not jump the face ahead of audible phrase delivery');
  const changes=h.calls.emotions.length;for(let i=0;i<20;i++)h.config.onTranscript({role:'assistant',itemId:'reply-1',delta:' ',final:false});assert.equal(h.calls.emotions.length,changes,'token arrival does not repeatedly trigger an expression');
  h.config.onLevel({input:1,output:0});assert.equal(h.calls.levels.at(-1),0,'microphone energy never makes the guide talk');
  h.state('listening');h.config.onLevel({input:1,output:.8});assert.equal(h.calls.levels.at(-1),0,'late output samples cannot keep an interrupted mouth moving');
  assert.equal(h.calls.starts,1);assert.equal(h.errors.length,0);
});

test('explicit grief or frustration keeps a gentle face through unrelated cheerful wording',async t=>{
  const h=fixture(t);await h.start();h.speak('A memorial for someone who died.');h.state('speaking');h.calls.emotions.length=0;h.config.onTranscript({role:'assistant',itemId:'reply-1',text:'Congratulations on your wedding!',final:true});assert.equal(h.calls.emotions.includes('celebrate'),false);assert.equal(h.calls.states.at(-1),'speaking');
  h.speak('Thanks, but this is not helpful and I am frustrated.','input-2',2);assert.equal(h.calls.emotions.at(-1),'reassuring');h.config.onTranscript({role:'assistant',itemId:'reply-2',text:'Which piece would you like to try?',final:true});assert.equal(h.calls.emotions.at(-1),'reassuring');
  h.speak('What is the meaning of the design?','input-3',3);assert.equal(h.calls.emotions.at(-1),'reassuring','a follow-up keeps the repair context');
  h.speak('All resolved. What is the meaning of the design?','input-4',4);assert.equal(h.calls.emotions.at(-1),'curious','an explicit resolution releases the repair context');
});

test('a suspended audio analyser has its own recoverable control while native voice stays connected',async t=>{
  const h=fixture(t);await h.start();h.state('speaking');const stops=h.calls.stops;h.meter('paused');assert.equal(h.button('Enable facial feedback').hidden,false);assert.equal(h.root.querySelector('.voice-state').textContent,'Your guide is speaking');assert.match(h.root.querySelector('.status').textContent,/sound-reactive face is paused/);
  h.config.onLevel({input:.5,output:.9});assert.equal(h.calls.levels.at(-1),0);h.button('Enable facial feedback').click();await settle();assert.equal(h.calls.resumes,1);assert.equal(h.calls.starts,1);assert.equal(h.calls.stops,stops);assert.equal(h.button('Enable facial feedback').hidden,false,'audio playback success alone cannot claim analyser recovery');
  h.meter('ready');assert.equal(h.button('Enable facial feedback').hidden,true);h.config.onLevel({input:.5,output:.75});assert.equal(h.calls.levels.at(-1),.75);assert.equal(h.root.querySelector('.status').textContent,'');
});

test('playback and facial analyser recover independently in either event order without another session',async t=>{
  const h=fixture(t);await h.start();h.state('speaking');h.meter('paused');h.client.playbackBlocked=true;h.config.onPlaybackBlocked();assert.equal(h.button('Enable audio').hidden,false);assert.match(h.root.querySelector('.voice-state').textContent,/audio paused/);
  h.client.playbackBlocked=false;h.config.onPlaybackResumed();assert.equal(h.button('Enable facial feedback').hidden,false);assert.equal(h.root.querySelector('.voice-state').textContent,'Your guide is speaking');h.meter('ready');assert.equal(h.button('Enable facial feedback').hidden,true);assert.equal(h.calls.starts,1);
});

test('a browser without a visual analyser keeps native audio and reports the exact visual limitation',async t=>{
  const h=fixture(t);await h.start();h.state('speaking');const stops=h.calls.stops;h.meter('unavailable');assert.equal(h.button('End voice').getAttribute('aria-pressed'),'true');assert.match(h.root.querySelector('.status').textContent,/Voice stays connected/);assert.equal(h.calls.stops,stops);h.config.onLevel({output:1});assert.equal(h.calls.levels.at(-1),0);assert.equal(h.button('Enable facial feedback').hidden,true);
});

test('end voice ignores late RMS, meter and semantic events and allows the safe network explanation',async t=>{
  const h=fixture(t);await h.start();h.state('speaking');h.button('End voice').click();const levels=h.calls.levels.length,emotions=h.calls.emotions.length;h.config.onOutputMeterState('paused');h.config.onLevel({output:.9});h.config.onTranscript({role:'assistant',itemId:'stale',text:'What would you like?',final:true});assert.equal(h.calls.levels.length,levels);assert.equal(h.calls.emotions.length,emotions);assert.equal(h.button('Enable facial feedback'),undefined);
  await h.start();h.config.onError('This browser could not prepare a voice network connection. Try another browser, or type here.');assert.match(h.root.querySelector('.status').textContent,/could not prepare a voice network connection/);assert.match(h.root.querySelector('.status').textContent,/choose Type instead/);
});

test('a bounded model gesture restores the baseline on expiry without a generated-text jump or stale revival',async t=>{
  const h=fixture(t);await h.start();h.speak('A birthday gift.');h.state('speaking');const performance={mood:'curious',gesture:'explain',intensity:.35,durationMs:1250},metadata={inputItemId:'input-1',turnVersion:1,currentTurn:true};h.config.onAvatarPerformance(performance,metadata);assert.equal(h.calls.performances.length,1);h.config.onTranscript({role:'assistant',itemId:'reply-1',text:'That could be a personal reminder.',final:true});assert.equal(h.calls.emotions.at(-1),'celebrate','the active finite gesture remains in charge until it finishes');h.advance(1250);h.calls.timers[0].fn();assert.equal(h.calls.emotions.at(-1),'celebrate','the baseline remains stable; audible clause expressions have their own layer');h.config.onAvatarPerformance(performance,metadata);const stale=h.calls.timers[1];h.button('End voice').click();const count=h.calls.emotions.length;h.advance(1250);stale.fn();assert.equal(h.calls.emotions.length,count);
});
