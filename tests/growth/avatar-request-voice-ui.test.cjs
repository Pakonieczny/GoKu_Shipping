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
  const calls={start:0,stop:0,level:[],resume:0},timers=[];let config,client;
  const set=w.setTimeout.bind(w),clear=w.clearTimeout.bind(w);w.setTimeout=(fn,ms)=>{if(ms===7000&&opts.captureTimers){const id=9000+timers.length;timers.push({fn,id});return id;}return set(fn,ms);};w.clearTimeout=id=>{if(id<9000)clear(id);};
  w.BritesConciergeAvatar={create:()=>({setState(){},setEmotion(){},setVisible(){},setLevel:v=>calls.level.push(v),triggerGreeting(){},cancelPerformance(){},destroy(){}})};
  w.BritesConciergeVoice={create(c){config=c;client={state:'idle',lastError:null,playbackBlocked:false,async start(){calls.start++;return opts.start?opts.start(client,c,calls.start):true;},stop(){calls.stop++;return opts.stop?opts.stop(client,c):Promise.resolve();},async resumeAudio(){calls.resume++;return opts.resume?opts.resume(client,c):true;},dispose:async()=>{},interrupt(){}};return client;}};
  w.fetch=async()=>({ok:true,json:async()=>({})});w.HTMLElement.prototype.scrollIntoView=function(){};w.eval(source);
  const root=d.querySelector('brites-concierge').shadowRoot,button=label=>[...root.querySelectorAll('button')].find(n=>n.textContent===label);
  async function activate(){button('Talk to me').click();await settle();root.querySelector('script[src$="brites-concierge-voice.js"]')?.dispatchEvent(new w.Event('load'));await settle();}
  t.after(()=>{opts.stop=null;w.BritesConcierge.close();w.close();});
  return {w,root,button,calls,timers,activate,open:()=>w.BritesConcierge.open(),get config(){return config;},get client(){return client;}};
}
test('preview sign-in explanation survives error, idle and stopped callbacks and start false',async t=>{
  const h=fixture(t,{start(client,c){client.lastError={message:'Sign in to the isolated preview before testing live voice.'};c.onError(client.lastError.message);c.onState('idle');c.onStopped('failed',{error:client.lastError});return false;}});
  h.open();await h.activate();assert.match(h.root.querySelector('.status').textContent,/requires operator sign-in/);assert.equal(h.root.querySelector('.voice-state').textContent,'Voice not connected');assert.equal(h.root.querySelector('.panel').dataset.voice,'error');assert.equal(h.button('Talk to me').getAttribute('aria-pressed'),'false');assert.equal(h.button('Talk to me').disabled,false);assert.equal(h.root.querySelector('.status').dataset.voiceError,'true');
});
test('failure without onError preserves the adapter fixed safe lastError',async t=>{
  const h=fixture(t,{start(client){client.lastError={message:'No microphone was found. Connect or enable a microphone, or type here.'};return false;}});h.open();await h.activate();assert.match(h.root.querySelector('.status').textContent,/No microphone/);assert.equal(h.root.querySelector('.voice-state').textContent,'Voice not connected');
});
test('untrusted structured lastError cannot expose account detail',async t=>{
  const h=fixture(t,{start(client){client.lastError={message:'private-account-id rejected private-credential',code:'secret'};return false;}});h.open();await h.activate();assert.equal(h.root.querySelector('.status').textContent,'Voice is unavailable right now. Try again or choose Type instead.');assert.doesNotMatch(h.root.textContent,/private-account|private-credential/);
});
test('connection feedback distinguishes availability and microphone permission',async t=>{
  const gate=deferred(),h=fixture(t,{start:()=>gate.promise});h.open();await h.activate();h.config.onConnectionPhase('checking');assert.match(h.root.querySelector('.voice-state').textContent,/Checking voice availability/);h.config.onConnectionPhase('microphone');assert.match(h.root.querySelector('.voice-state').textContent,/Allow microphone/);h.config.onConnectionPhase('connecting');assert.equal(h.root.querySelector('.voice-state').textContent,'Connecting your voice…');h.button('Cancel connection').click();gate.resolve(true);await settle();assert.equal(h.button('Talk to me').getAttribute('aria-pressed'),'false');
});
test('microphone energy cannot animate the guide as speaking output',async t=>{
  const h=fixture(t);h.open();await h.activate();h.config.onLevel({input:1,output:0});assert.equal(h.calls.level.at(-1),0);h.config.onLevel({input:0,output:.24});assert.equal(h.calls.level.at(-1),.24);h.config.onLevel({input:1,output:NaN});assert.equal(h.calls.level.at(-1),0);
});
test('blocked playback exposes a gesture recovery control without stopping media',async t=>{
  const h=fixture(t);h.open();await h.activate();const before=h.calls.stop;h.config.onState('speaking');h.client.state='speaking';h.config.onPlaybackBlocked();assert.equal(h.root.querySelector('.voice-state').textContent,'Voice connected · audio paused');assert.equal(h.button('Enable audio').hidden,false);assert.equal(h.button('End voice').getAttribute('aria-pressed'),'true');assert.equal(h.calls.stop,before);h.config.onState('speaking');assert.match(h.root.querySelector('.voice-state').textContent,/audio paused/);h.config.onLevel({output:1});assert.equal(h.calls.level.at(-1),0);h.button('Enable audio').click();await settle();assert.equal(h.calls.resume,1);assert.equal(h.button('Enable audio').hidden,true);assert.equal(h.root.querySelector('.voice-state').textContent,'Your guide is speaking');assert.equal(h.calls.stop,before);
});
test('playback gate during start cannot be overwritten by connection success',async t=>{
  const h=fixture(t,{start(client,c){client.playbackBlocked=true;c.onPlaybackBlocked();c.onState('listening');c.onConnectionPhase('connected');return true;}});h.open();await h.activate();assert.equal(h.root.querySelector('.panel').dataset.voice,'audio-paused');assert.equal(h.button('Enable audio').hidden,false);assert.match(h.root.querySelector('.voice-state').textContent,/audio paused/);
});
test('late resume result after End voice cannot resurrect connected UI',async t=>{
  const gate=deferred(),h=fixture(t,{resume:()=>gate.promise});h.open();await h.activate();h.config.onPlaybackBlocked();h.button('Enable audio').click();h.button('End voice').click();gate.resolve(true);await settle();assert.equal(h.root.querySelector('.panel').dataset.voice,'idle');assert.equal(h.button('Enable audio').hidden,true);assert.equal(h.button('Talk to me').getAttribute('aria-pressed'),'false');
});
test('immediate restart waits for prior cleanup and old stopped callback cannot reset the new attempt',async t=>{
  const gate=deferred(),opts={stop:()=>gate.promise},h=fixture(t,opts);h.open();await h.activate();h.button('End voice').click();h.button('Talk to me').click();await settle();assert.equal(h.calls.start,1);assert.match(h.root.querySelector('.voice-state').textContent,/Finishing the previous/);h.config.onStopped('user',{error:null});gate.resolve();await settle();assert.equal(h.calls.start,2);assert.ok(h.button('End voice'));assert.equal(h.root.querySelector('.panel').dataset.voice,'connecting');
});
test('missing script has a bounded failure and removes the obsolete loader',async t=>{
  const h=fixture(t,{captureTimers:true});h.open();h.button('Talk to me').click();await settle();const timer=h.timers.find(t=>t.fn);assert.ok(timer);timer.fn();await settle();assert.match(h.root.querySelector('.status').textContent,/Voice is unavailable/);assert.equal(h.root.querySelector('script[src$="brites-concierge-voice.js"]'),null);assert.equal(h.button('Talk to me').disabled,false);assert.equal(h.calls.start,0);
});
test('exact isolated voice preview uses the explicit existing operator sign-in without caching or displaying it',async t=>{
  const h=fixture(t,{url:'https://brites-growth-sandbox.netlify.app/concierge-sandbox.html'});h.w.sessionStorage.setItem('brites-growth-key','synthetic-operator-a');h.open();await h.activate();
  assert.deepEqual({...h.config.headers()},{'X-Growth-Key':'synthetic-operator-a'});assert.doesNotMatch(h.root.textContent,/synthetic-operator/);
  h.w.sessionStorage.setItem('brites-growth-key','synthetic-operator-b');assert.equal(h.config.headers()['X-Growth-Key'],'synthetic-operator-b');
  h.w.sessionStorage.removeItem('brites-growth-key');assert.deepEqual({...h.config.headers()},{});
});
test('operator voice header never leaves the exact preview origin, path and API boundary',async t=>{
  for(const opts of [{url:'https://britesjewelry.com/'},{url:'https://preview.example/concierge-sandbox.html'},{url:'https://brites-growth-sandbox.netlify.app/concierge-actions-qa.html'},{url:'https://brites-growth-sandbox.netlify.app/concierge-sandbox.html',api:'https://foreign.example/'},{url:'https://brites-growth-sandbox.netlify.app/concierge-sandbox.html',sandbox:false}]){
    const h=fixture(t,opts);h.w.sessionStorage.setItem('brites-growth-key','synthetic-operator');h.open();await h.activate();assert.deepEqual({...h.config.headers()},{});
  }
});
test('missing, malformed or unavailable preview sign-in storage stays safely unauthenticated',async t=>{
  const h=fixture(t,{url:'https://brites-growth-sandbox.netlify.app/concierge-sandbox.html'});h.open();await h.activate();
  for(const value of ['', '  ', 'bad\nheader', 'x'.repeat(1025)]){h.w.sessionStorage.setItem('brites-growth-key',value);assert.deepEqual({...h.config.headers()},{});}
  Object.defineProperty(h.w,'sessionStorage',{get(){throw Error('storage unavailable');}});assert.deepEqual({...h.config.headers()},{});
});
