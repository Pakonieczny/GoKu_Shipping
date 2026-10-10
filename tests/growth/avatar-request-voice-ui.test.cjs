'use strict';
// Real widget UI; synthetic adapter only. Does not certify microphone/playback.
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs');
const {JSDOM}=require('jsdom');
const source=fs.readFileSync(require.resolve('../../brites-concierge.js'),'utf8');
const tick=()=>new Promise(resolve=>setImmediate(resolve));
async function settle(){await tick();await tick();}
function deferred(){let resolve;const promise=new Promise(r=>{resolve=r;});return {promise,resolve};}
function fixture(t,opts={}){
  const dom=new JSDOM('<!doctype html><body></body>',{url:opts.url||'https://preview.example/concierge-sandbox.html',pretendToBeVisual:true,runScripts:'outside-only'}),w=dom.window,d=w.document;
  const script=d.createElement('script');script.src='/brites-concierge.js';script.dataset.sandbox=opts.sandbox===false?'false':'true';if(opts.api)script.dataset.api=opts.api;Object.defineProperty(d,'currentScript',{get:()=>script});Object.defineProperty(d,'hidden',{get:()=>false});
  const calls={start:0,stop:0,level:[],resume:0,requests:[],states:[]},timers=[];let config,client;
  const set=w.setTimeout.bind(w),clear=w.clearTimeout.bind(w);w.setTimeout=(fn,ms)=>{if(ms===7000&&opts.captureTimers){const id=9000+timers.length;timers.push({fn,id});return id;}return set(fn,ms);};w.clearTimeout=id=>{if(id<9000)clear(id);};
  w.BritesConciergeAvatar={create:()=>({setState:v=>calls.states.push(v),setEmotion(){},setVisible(){},setLevel:v=>calls.level.push(v),triggerGreeting(){},cancelPerformance(){},destroy(){}})};
  w.BritesConciergeVoice={create(c){config=c;client={state:'idle',lastError:null,playbackBlocked:false,async start(){calls.start++;return opts.start?opts.start(client,c,calls.start):true;},stop(){calls.stop++;return opts.stop?opts.stop(client,c):Promise.resolve();},async resumeAudio(){calls.resume++;return opts.resume?opts.resume(client,c):true;},dispose:async()=>{},interrupt(){}};return client;}};
  w.fetch=async(url,init)=>{calls.requests.push({url,init});return opts.fetch?opts.fetch(url,init):{ok:true,json:async()=>({})};};w.HTMLElement.prototype.scrollIntoView=function(){};w.eval(source);
  const root=d.querySelector('brites-concierge').shadowRoot,button=label=>[...root.querySelectorAll('button')].find(n=>n.textContent===label);
  async function activate(){button('Talk to me').click();await settle();root.querySelector('script[src$="brites-concierge-voice.js"]')?.dispatchEvent(new w.Event('load'));await settle();}
  t.after(()=>{opts.stop=null;w.BritesConcierge.close();w.close();});
  return {w,root,button,calls,timers,activate,open:()=>w.BritesConcierge.open(),get config(){return config;},get client(){return client;}};
}
test('preview sign-in explanation survives error, idle and stopped callbacks and start false',async t=>{
  const h=fixture(t,{start(client,c){client.lastError={message:'Sign in to the isolated preview before testing live voice.'};c.onError(client.lastError.message);c.onState('idle');c.onStopped('failed',{error:client.lastError});return false;}});
  h.open();await h.activate();assert.match(h.root.querySelector('.status').textContent,/requires operator sign-in/);assert.equal(h.root.querySelector('.voice-state').textContent,'Voice not connected');assert.equal(h.root.querySelector('.panel').dataset.voice,'error');assert.equal(h.button('Talk to me').getAttribute('aria-pressed'),'false');assert.equal(h.button('Talk to me').disabled,false);assert.equal(h.root.querySelector('.status').dataset.voiceError,'true');
});
const previewUrl='https://brites-growth-sandbox.netlify.app/concierge-sandbox.html';
const availabilityResponse=(data,status=200)=>({ok:status>=200&&status<300,status,json:async()=>data});
function capabilitiesOnly(fetcher){return (url,init)=>url.endsWith('/api/concierge-voice')?fetcher(JSON.parse(init.body),init):availabilityResponse({});}
test('opening the isolated preview shows an allocation pause without starting media or showing a connection failure',async t=>{
  const h=fixture(t,{url:previewUrl,fetch:capabilitiesOnly(body=>{assert.equal(body.action,'capabilities');return availabilityResponse({enabled:false,code:'VOICE_ALLOCATION_UNAVAILABLE',message:'private-account-info'},429);})});
  assert.equal(h.calls.requests.length,0);h.open();await settle();
  assert.equal(h.root.querySelector('.voice-state').textContent,'Voice paused');assert.equal(h.root.querySelector('.panel').dataset.voice,'unavailable');assert.match(h.root.querySelector('.status').textContent,/allocation is paused/);assert.doesNotMatch(h.root.textContent,/private-account/);
  assert.ok(h.button('Check voice availability'));assert.equal(h.button('Check voice availability').getAttribute('aria-pressed'),'false');assert.equal(h.calls.start,0);assert.equal(h.root.querySelector('script[src$="brites-concierge-voice.js"]'),null);assert.equal(h.calls.states.includes('error'),false);
  h.button('Check voice availability').click();await settle();assert.equal(h.calls.requests.filter(r=>r.url.endsWith('/api/concierge-voice')).length,2);assert.equal(h.calls.start,0);
  h.button('Type instead').click();assert.equal(h.root.querySelector('.composer').hidden,false);
});
test('availability recovery offers Talk to me and still requires a separate explicit start',async t=>{
  let checks=0;const h=fixture(t,{url:previewUrl,fetch:capabilitiesOnly(()=>availabilityResponse(++checks===1?{enabled:false,code:'VOICE_ALLOCATION_UNAVAILABLE'}:{enabled:true},checks===1?429:200))});
  h.open();await settle();h.button('Check voice availability').click();await settle();
  assert.ok(h.button('Talk to me'));assert.equal(h.calls.start,0);assert.equal(h.root.querySelector('.voice-state').textContent,'Voice available \u00b7 select Talk to me');assert.equal(h.root.querySelector('.status').textContent,'');
  await h.activate();assert.equal(h.calls.start,1);assert.ok(h.button('End voice'));
});
test('a late availability refusal cannot overwrite a newer explicit connection',async t=>{
  const gate=deferred(),h=fixture(t,{url:previewUrl,fetch:capabilitiesOnly(()=>gate.promise)});
  h.open();await h.activate();assert.ok(h.button('End voice'));assert.equal(h.calls.start,1);
  gate.resolve(availabilityResponse({enabled:false,code:'VOICE_ALLOCATION_UNAVAILABLE'},429));await settle();
  assert.ok(h.button('End voice'));assert.doesNotMatch(h.root.querySelector('.voice-state').textContent,/paused|not connected/);assert.equal(h.root.querySelector('.status').textContent,'');
});
test('dismissal aborts availability and a stale refusal cannot overwrite a reopened preview',async t=>{
  const gate=deferred();let checks=0,firstSignal;const h=fixture(t,{url:previewUrl,fetch:capabilitiesOnly((body,init)=>{if(++checks===1){firstSignal=init.signal;return gate.promise;}return availabilityResponse({enabled:true});})});
  h.open();await settle();h.w.BritesConcierge.close();assert.equal(firstSignal.aborted,true);h.open();await settle();
  gate.resolve(availabilityResponse({enabled:false,code:'VOICE_ALLOCATION_UNAVAILABLE'},429));await settle();assert.ok(h.button('Talk to me'));assert.equal(h.root.querySelector('.panel').dataset.voice,'idle');assert.equal(h.root.querySelector('.status').textContent,'');assert.equal(h.calls.start,0);
});
test('unknown availability responses stay private and explicit rechecking remains recoverable',async t=>{
  let checks=0;const h=fixture(t,{url:previewUrl,fetch:capabilitiesOnly(()=>availabilityResponse(++checks===1?{enabled:false,code:'VOICE_DISABLED'}:{enabled:false,code:'private-account-id',message:'private-credential'},503))});
  h.open();await settle();assert.equal(h.root.querySelector('.voice-state').textContent,'Voice unavailable');h.button('Check voice availability').click();await settle();
  assert.equal(h.button('Check voice availability').disabled,false);assert.match(h.root.querySelector('.status').textContent,/could not be checked/);assert.doesNotMatch(h.root.textContent,/private-account|private-credential/);assert.equal(h.calls.start,0);
});
test('a start rejected by the allocation guard offers a read-only recheck without changing the avatar into an error',async t=>{
  const h=fixture(t,{start(client,c){client.lastError={message:'This preview\u2019s voice allocation is paused. You can still type.'};c.onError(client.lastError.message);c.onState('idle');c.onStopped('failed',{error:client.lastError});return false;},fetch:capabilitiesOnly(()=>availabilityResponse({enabled:true}))});
  h.open();await h.activate();assert.equal(h.root.querySelector('.voice-state').textContent,'Voice paused');assert.ok(h.button('Check voice availability'));assert.equal(h.calls.states.includes('error'),false);
  h.button('Check voice availability').click();await settle();assert.ok(h.button('Talk to me'));assert.equal(h.calls.start,1);
});
for(const finishBeforeRecovery of [false,true])test('voice recovery preserves '+(finishBeforeRecovery?'completed':'pending')+' typed selection feedback',async t=>{
  const voiceGate=deferred(),selectionGate=deferred();let checks=0;
  const h=fixture(t,{url:previewUrl,fetch:(url,init)=>{
    if(url.endsWith('/api/concierge-voice'))return ++checks===1?availabilityResponse({enabled:false,code:'VOICE_ALLOCATION_UNAVAILABLE'},429):voiceGate.promise;
    if(JSON.parse(init?.body||'{}').message)return selectionGate.promise;
    return availabilityResponse({});
  }});
  h.open();await settle();h.button('Type instead').click();const input=h.root.querySelector('.composer input');input.value='A gift for a friend';h.root.querySelector('form').dispatchEvent(new h.w.Event('submit',{bubbles:true,cancelable:true}));await settle();
  assert.equal(h.root.querySelector('.status').textContent,'Checking the live selection\u2026');h.button('Check voice availability').click();await settle();
  if(finishBeforeRecovery){selectionGate.resolve(availabilityResponse({reply:'What kind of piece do they like?',products:[],preferences:{},question:null}));await settle();}
  const ownedFeedback=h.root.querySelector('.status').textContent;voiceGate.resolve(availabilityResponse({enabled:true}));await settle();assert.equal(h.root.querySelector('.status').textContent,ownedFeedback);assert.ok(h.button('Talk to me'));
  if(!finishBeforeRecovery){selectionGate.resolve(availabilityResponse({reply:'What kind of piece do they like?',products:[],preferences:{},question:null}));await settle();}
});
test('failure without onError preserves the adapter fixed safe lastError',async t=>{
  const h=fixture(t,{start(client){client.lastError={message:'No microphone was found. Connect or enable a microphone, or type here.'};return false;}});h.open();await h.activate();assert.match(h.root.querySelector('.status').textContent,/No microphone/);assert.equal(h.root.querySelector('.voice-state').textContent,'Voice not connected');
});
test('untrusted structured lastError cannot expose account detail',async t=>{
  const h=fixture(t,{start(client){client.lastError={message:'private-account-id rejected private-credential',code:'secret'};return false;}});h.open();await h.activate();assert.equal(h.root.querySelector('.status').textContent,'Voice is unavailable right now. Try again or choose Type instead.');assert.doesNotMatch(h.root.textContent,/private-account|private-credential/);
});
test('connection feedback distinguishes availability and microphone permission',async t=>{
  const gate=deferred(),h=fixture(t,{start:()=>gate.promise});h.open();await h.activate();h.config.onConnectionPhase('checking');assert.match(h.root.querySelector('.voice-state').textContent,/Checking voice availability/);h.config.onConnectionPhase('microphone');assert.match(h.root.querySelector('.voice-state').textContent,/Allow microphone/);h.config.onConnectionPhase('connecting');assert.equal(h.root.querySelector('.voice-state').textContent,'Connecting your voice\u2026');h.button('Cancel connection').click();gate.resolve(true);await settle();assert.equal(h.button('Talk to me').getAttribute('aria-pressed'),'false');
});
test('microphone energy cannot animate the guide as speaking output',async t=>{
  const h=fixture(t);h.open();await h.activate();h.config.onState('listening');h.config.onLevel({input:1,output:0});assert.equal(h.calls.level.at(-1),0);h.config.onLevel({input:0,output:.24});assert.equal(h.calls.level.at(-1),0,'output samples do not move a mouth while the guide is listening');h.config.onState('speaking');h.config.onLevel({input:1,output:.24});assert.equal(h.calls.level.at(-1),.24,'only current guide output drives speaking feedback');h.config.onLevel({input:1,output:NaN});assert.equal(h.calls.level.at(-1),0);h.config.onState('listening');h.config.onLevel({input:0,output:1});assert.equal(h.calls.level.at(-1),0,'interruption silences late output energy');
});
test('blocked playback exposes a gesture recovery control without stopping media',async t=>{
  const h=fixture(t);h.open();await h.activate();const recovery=h.root.querySelector('.voice-recovery'),before=h.calls.stop;h.config.onState('speaking');h.client.state='speaking';h.config.onPlaybackBlocked();assert.equal(h.root.querySelector('.voice-state').textContent,'Voice connected \u00b7 audio paused');assert.equal(h.button('Enable audio').hidden,false);assert.equal(h.button('End voice').getAttribute('aria-pressed'),'true');assert.equal(h.calls.stop,before);h.config.onState('speaking');assert.match(h.root.querySelector('.voice-state').textContent,/audio paused/);h.config.onLevel({output:1});assert.equal(h.calls.level.at(-1),0);recovery.click();await settle();assert.equal(h.calls.resume,1);assert.equal(recovery.hidden,true);assert.equal(h.root.querySelector('.voice-recovery'),recovery,'recovery preserves the same control after its label changes');assert.equal(h.root.querySelector('.voice-state').textContent,'Your guide is speaking');assert.equal(h.calls.stop,before);
});
test('playback gate during start cannot be overwritten by connection success',async t=>{
  const h=fixture(t,{start(client,c){client.playbackBlocked=true;c.onPlaybackBlocked();c.onState('listening');c.onConnectionPhase('connected');return true;}});h.open();await h.activate();assert.equal(h.root.querySelector('.panel').dataset.voice,'audio-paused');assert.equal(h.button('Enable audio').hidden,false);assert.match(h.root.querySelector('.voice-state').textContent,/audio paused/);
});
test('late resume result after End voice cannot resurrect connected UI',async t=>{
  const gate=deferred(),h=fixture(t,{resume:()=>gate.promise});h.open();await h.activate();const recovery=h.root.querySelector('.voice-recovery');h.config.onPlaybackBlocked();recovery.click();h.button('End voice').click();gate.resolve(true);await settle();assert.equal(h.root.querySelector('.panel').dataset.voice,'idle');assert.equal(recovery.hidden,true);assert.equal(h.button('Talk to me').getAttribute('aria-pressed'),'false');
});
test('immediate restart waits for prior cleanup and old stopped callback cannot reset the new attempt',async t=>{
  const gate=deferred(),opts={stop:()=>gate.promise},h=fixture(t,opts);h.open();await h.activate();h.button('End voice').click();h.button('Talk to me').click();await settle();assert.equal(h.calls.start,1);assert.match(h.root.querySelector('.voice-state').textContent,/Finishing the previous/);h.config.onStopped('user',{error:null});gate.resolve();await settle();assert.equal(h.calls.start,2);assert.ok(h.button('End voice'));assert.equal(h.root.querySelector('.panel').dataset.voice,'connecting');
});
test('missing native voice script has a bounded failure and removes its obsolete loader',async t=>{
  const h=fixture(t,{captureTimers:true});h.open();
  // Opening starts an independent optional expression asset. Its timeout must
  // not be mistaken for the native voice loader created by the Talk request.
  const openingTimers=new Set(h.timers);h.button('Talk to me').click();await settle();const loader=h.root.querySelector('script[src$="brites-concierge-voice.js"]'),timer=h.timers.find(value=>!openingTimers.has(value));assert.ok(loader);assert.ok(timer);timer.fn();await settle();assert.match(h.root.querySelector('.status').textContent,/Voice is unavailable/);assert.equal(loader.isConnected,false);assert.equal(h.root.querySelector('script[src$="brites-concierge-voice.js"]'),null);assert.equal(h.button('Talk to me').disabled,false);assert.equal(h.calls.start,0);
});
for(const pathname of ['/concierge-sandbox','/concierge-sandbox.html'])test('exact isolated voice preview reads availability and reuses private operator sign-in at '+pathname,async t=>{
  const h=fixture(t,{url:'https://brites-growth-sandbox.netlify.app'+pathname,fetch:capabilitiesOnly(()=>availabilityResponse({enabled:true,message:'PRIVATE_AVAILABILITY_DETAIL'}))});h.w.sessionStorage.setItem('brites-growth-key','synthetic-operator-a');h.open();await settle();
  const checks=h.calls.requests.filter(r=>r.url.endsWith('/api/concierge-voice'));assert.equal(checks.length,1);assert.deepEqual(JSON.parse(checks[0].init.body),{action:'capabilities'});assert.equal(checks[0].init.headers['X-Growth-Key'],'synthetic-operator-a');assert.equal(checks[0].init.credentials,'same-origin');assert.equal(checks[0].init.redirect,'error');assert.equal(checks[0].init.cache,'no-store');assert.equal(h.calls.start,0);assert.equal(h.root.querySelector('script[src$="brites-concierge-voice.js"]'),null);
  await h.activate();assert.equal(h.calls.start,1);assert.deepEqual({...h.config.headers()},{'X-Growth-Key':'synthetic-operator-a'});assert.doesNotMatch(h.root.innerHTML,/synthetic-operator|PRIVATE_AVAILABILITY_DETAIL/);assert.doesNotMatch(JSON.stringify({context:h.config.getContext(),memory:h.config.getMemory()}),/synthetic-operator|PRIVATE_AVAILABILITY_DETAIL|X-Growth-Key/);assert.ok(h.calls.requests.every(r=>!String(r.init?.body||'').includes('synthetic-operator')));
  h.w.sessionStorage.setItem('brites-growth-key','synthetic-operator-b');assert.equal(h.config.headers()['X-Growth-Key'],'synthetic-operator-b');
  h.w.sessionStorage.removeItem('brites-growth-key');assert.deepEqual({...h.config.headers()},{});
});
test('operator voice header never leaves the exact preview origin, path and API boundary',async t=>{
  const origin='https://brites-growth-sandbox.netlify.app',excluded=[
    {url:'https://britesjewelry.com/'},
    ...['/concierge-sandbox/','/concierge-sandbox.html/','/concierge-sandbox-extra','/concierge-sandbox.html.backup','/concierge-sandbox/other','/concierge-actions-qa.html'].map(path=>({url:origin+path})),
    ...['/concierge-sandbox','/concierge-sandbox.html'].flatMap(path=>[{url:'https://preview.example'+path},{url:'https://brites-growth-sandbox.netlify.app.foreign.test'+path},{url:'http://brites-growth-sandbox.netlify.app'+path},{url:origin+path,api:'https://foreign.example/'},{url:origin+path,sandbox:false}])
  ];
  for(const opts of excluded){
    const h=fixture(t,opts);h.w.sessionStorage.setItem('brites-growth-key','synthetic-operator');h.open();await settle();assert.equal(h.calls.requests.filter(r=>r.url.endsWith('/api/concierge-voice')).length,0,JSON.stringify(opts));await h.activate();assert.deepEqual({...h.config.headers()},{},JSON.stringify(opts));assert.doesNotMatch(JSON.stringify({context:h.config.getContext(),memory:h.config.getMemory()}),/synthetic-operator|X-Growth-Key/);assert.doesNotMatch(h.root.innerHTML,/synthetic-operator/);
  }
});
for(const pathname of ['/concierge-sandbox','/concierge-sandbox.html'])test('missing, malformed or unavailable preview sign-in storage stays safely unauthenticated at '+pathname,async t=>{
  const h=fixture(t,{url:'https://brites-growth-sandbox.netlify.app'+pathname});h.open();await h.activate();
  for(const value of ['', '  ', 'bad\nheader', 'x'.repeat(1025)]){h.w.sessionStorage.setItem('brites-growth-key',value);assert.deepEqual({...h.config.headers()},{});}
  Object.defineProperty(h.w,'sessionStorage',{get(){throw Error('storage unavailable');}});assert.deepEqual({...h.config.headers()},{});
});

test('a meter diagnostic during network setup cannot claim that voice is already connected',async t=>{
  const gate=deferred(),h=fixture(t,{async start(client,c){client.state='connecting';c.onState('connecting');await gate.promise;client.state='listening';c.onState('listening');return true;}});h.open();await h.activate();h.config.onOutputMeterState('unavailable');assert.equal(h.root.querySelector('.panel').dataset.voice,'connecting');assert.equal(h.root.querySelector('.voice-state').textContent,'Connecting your voice\u2026');assert.doesNotMatch(h.root.querySelector('.status').textContent,/Voice stays connected/);gate.resolve();await settle();assert.equal(h.root.querySelector('.panel').dataset.voice,'listening');assert.match(h.root.querySelector('.status').textContent,/Voice stays connected/);assert.equal(h.calls.start,1);
});
