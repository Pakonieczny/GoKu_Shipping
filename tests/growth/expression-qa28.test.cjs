'use strict';
// Production planner and actual fallback rig with authored media events. These
// checks do not certify physical audio, GPU appearance, or perceived trust.
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs');
const {JSDOM}=require('jsdom');
const qa=require('../../concierge-expression-qa.js'),expression=require('../../brites-concierge-expression.js'),avatarFactory=require('../../brites-concierge-avatar.js');

async function fixture(t,{reduced=false}={}){
  const dom=new JSDOM('<div id="mount"></div>',{url:'https://sandbox.example/concierge-expression-qa.html',pretendToBeVisual:true}),win=dom.window;
  let clock=0;Object.defineProperty(win.performance,'now',{value:()=>clock});
  win.matchMedia=()=>({matches:reduced,addEventListener(){},removeEventListener(){}});
  const guide=avatarFactory.create({container:win.document.getElementById('mount'),visible:true,greetingOnOpen:false,loadScene:async()=>{throw Error('Synthetic unavailable WebGL');}});
  await guide.ready;const completed=[],runner=qa.createRunner({expressionFactory:expression,avatar:guide,now:()=>clock,onComplete:value=>completed.push(value)});
  t.after(()=>{runner.destroy();guide.destroy();win.close();});
  return {runner,guide,completed,advance(value){clock=value;return runner.update(value);},run(scenario){runner.start(scenario,{reducedMotion:reduced});for(let at=0;at<=scenario.duration;at+=50){clock=at;runner.update(at);}return completed.at(-1);}};
}

test('QA owns sixteen bounded authored scenario tapes without media, inference or shopper writes',()=>{
  assert.equal(qa.SCENARIOS.length,16);assert.equal(new Set(qa.SCENARIOS.map(value=>value.id)).size,16);
  for(const scenario of qa.SCENARIOS){const tape=qa.tapeFor(scenario);assert.ok(tape.length<12);assert.equal(tape[0].type,'begin');assert.equal(tape.at(-1).type,'finish');assert.ok(tape.every((event,index)=>Number.isFinite(event.at)&&event.at>=0&&event.at<=scenario.duration&&(!index||event.at>=tape[index-1].at)));}
  assert.throws(()=>qa.tapeFor({id:'untrusted',duration:Infinity}),/authored scenario/);
});

test('buffered final text cannot select the future question face before playback, then multiple real channels change',async t=>{
  const h=await fixture(t),scenario=qa.SCENARIOS.find(value=>value.id==='phrases');h.runner.start(scenario);h.advance(100);h.advance(800);
  const before=h.runner.snapshot();assert.equal(before.outputRms,0);assert.equal(before.controller.cue,null);assert.equal(before.face.expression,null);assert.equal(before.face.mouthEnergy,0);
  for(let at=850;at<=scenario.duration;at+=50)h.advance(at);
  const result=h.completed.at(-1);assert.equal(result.invariants.beforePlaybackNoSpeechCue,true);assert.equal(result.multipleCueCheck,true);assert.ok(result.distinctCueKinds.includes('celebrate'));assert.ok(result.distinctCueKinds.includes('inquiry'));assert.ok(result.changedFaceChannels.includes('brows'));assert.ok(result.changedFaceChannels.includes('eyes'));assert.ok(result.distinctMouthEnergySamples>2);assert.equal(result.rendererMode,'fallback');assert.equal(result.gpuAppearance,'unverified');assert.equal(result.nativeSpokenTiming,'unverified');
});

test('every authored scenario reaches its terminal result through the production planner and actual rig',async t=>{
  const h=await fixture(t);
  for(const scenario of qa.SCENARIOS){const result=h.run(scenario);assert.equal(result.scenario,scenario.id);assert.equal(result.providerCalls,0);assert.equal(result.microphoneRequests,0);assert.equal(result.invariants.finalOutputRmsZero,true);assert.deepEqual(result.failedInvariants,[],scenario.id+' failed a lifecycle/media invariant');if(scenario.check==='multiple-cues')assert.equal(result.multipleCueCheck,true,scenario.id+' did not visibly exercise multiple communicative face targets');if(scenario.role==='user')assert.equal(result.invariants.listeningMouthClosed,true);if(scenario.analysisUnavailable)assert.equal(result.invariants.analysisUnavailableNoFakeMouth,true);}
});

test('interruption and late old response do not resurrect a speaking face or energy',async t=>{
  const h=await fixture(t),scenario=qa.SCENARIOS.find(value=>value.id==='late'),result=h.run(scenario);assert.equal(result.invariants.interruptionStopsMouth,true);assert.equal(h.runner.snapshot().face.state,'listening');assert.equal(h.runner.snapshot().face.mouthEnergy,0);assert.equal(h.runner.snapshot().controller.cue,null);
});

test('listener support begins only after available words, not from the unknown future fixture text',async t=>{
  const h=await fixture(t),scenario=qa.SCENARIOS.find(value=>value.id==='listen-support');h.runner.start(scenario);
  for(let at=0;at<=3300;at+=50)h.advance(at);const before=h.runner.snapshot();assert.equal(before.controller.cue.kind,'attentive');assert.equal(before.face.expression.kind,'attentive');assert.equal(before.face.mouthEnergy,0);assert.equal(before.cues.some(value=>value.kind==='support'),false);
  h.advance(3400);const after=h.runner.snapshot();assert.equal(after.controller.cue.kind,'support');assert.equal(after.face.expression.kind,'support');assert.equal(after.face.mouthEnergy,0);assert.equal(after.cues.some(value=>value.kind==='support'),true);
});

test('unavailable analysis and reduced motion keep all synthetic output energy and aperture at zero',async t=>{
  const h=await fixture(t,{reduced:true}),result=h.run(qa.SCENARIOS.find(value=>value.id==='reduced'));assert.equal(result.invariants.reducedNoMovingMouth,true);assert.equal(h.runner.snapshot().face.reducedMotion,true);assert.equal(h.guide.snapshot().mannerism.active,false);assert.equal(h.guide.snapshot().facePose.speechEnergy,0);
});

test('pause/reset clear active expression and cannot continue an old fixture run',async t=>{
  const h=await fixture(t),scenario=qa.SCENARIOS[0];h.runner.start(scenario);for(let at=0;at<1600;at+=50)h.advance(at);assert.ok(h.runner.snapshot().controller.cue);h.runner.pause(true);assert.equal(h.runner.snapshot().face.paused,true);assert.equal(h.runner.snapshot().face.expression,null);assert.equal(h.runner.snapshot().face.mouthEnergy,0);h.advance(8000);assert.equal(h.runner.snapshot().clockMs,1550);h.runner.reset();h.advance(11000);assert.equal(h.runner.snapshot().running,false);assert.equal(h.runner.snapshot().controller.cue,null);assert.equal(h.runner.snapshot().face.expression,null);
});

test('the explicit QA script stays inert on the ordinary shopper route',t=>{
  const dom=new JSDOM('<!doctype html><body><div id="avatar"></div></body>',{url:'https://sandbox.example/concierge-sandbox.html',runScripts:'outside-only'}),win=dom.window;let created=0,requests=0;win.BritesConciergeAvatar={create(){created++;throw Error('Do not initialize fixture here');}};win.fetch=()=>{requests++;};win.eval(fs.readFileSync(require.resolve('../../concierge-expression-qa.js'),'utf8'));t.after(()=>win.close());assert.equal(created,0);assert.equal(requests,0);assert.equal(win.document.querySelector('#avatar').childElementCount,0);
});
