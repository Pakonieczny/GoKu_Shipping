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
  for(const scenario of qa.SCENARIOS){const result=h.run(scenario);assert.equal(result.scenario,scenario.id);assert.equal(result.providerCalls,0);assert.equal(result.microphoneRequests,0);assert.equal(result.invariants.finalOutputRmsZero,true);assert.deepEqual(result.failedInvariants,[],scenario.id+' failed a lifecycle/media invariant');if(scenario.check==='multiple-cues')assert.equal(result.multipleCueCheck,true,scenario.id+' did not exercise multiple source cues and observed face channels');if(scenario.role==='user')assert.equal(result.invariants.listeningMouthClosed,true);if(scenario.analysisUnavailable)assert.equal(result.invariants.analysisUnavailableNoFakeMouth,true);if(['phrases','support','repair','mixed','uncertain','negation','generation'].includes(scenario.id))assert.equal(result.invariants.finalInquiryCueReached,true,scenario.id+' did not reach its final question cue');}
});

test('sparse browser updates preserve authored audio and final question coverage without inventing face frames',async t=>{
  const dense=await fixture(t),sparse=await fixture(t);
  for(const id of ['phrases','support','uncertain','generation']){
    const scenario=qa.SCENARIOS.find(value=>value.id===id),normal=dense.run(scenario);
    sparse.runner.start(scenario);
    for(let at=1000;at<scenario.duration;at+=1000)sparse.advance(at);
    sparse.advance(scenario.duration);
    const slow=sparse.completed.at(-1);
    assert.deepEqual(slow.distinctCueKinds,normal.distinctCueKinds,id+' source cue sequence depends on renderer cadence');
    assert.deepEqual(slow.sourcePhraseIndices,normal.sourcePhraseIndices,id+' source phrase coverage depends on renderer cadence');
    assert.deepEqual(slow.sourceEventCoverage,qa.tapeFor(scenario).map(event=>({at:event.at,type:event.type})),id+' event must occur at its authored timestamp');
    assert.equal(slow.sourceSampleCount,normal.sourceSampleCount);
    assert.equal(slow.sourcePlaybackSamples,normal.sourcePlaybackSamples);
    assert.equal(slow.invariants.finalInquiryCueReached,true);
    assert.ok(slow.actualRenderedFrameSampleCount<=Math.ceil(scenario.duration/1000)+1);
    assert.ok(slow.actualRenderedFrameSampleCount<normal.actualRenderedFrameSampleCount/5);
    assert.ok(slow.sourcePlaybackSamples>slow.actualRenderedFrameSampleCount*10);
    assert.equal(slow.maxObservedFrameGapMs,1000);
    assert.equal(slow.visibleTrajectoryCoverage,'incomplete-sparse-observations');
    assert.equal(slow.result,'source-check-passed-visible-coverage-incomplete');
    assert.equal(slow.gpuAppearance,'unverified');
  }
});

test('one terminal browser update drains only a bounded tape and reports two actual pose observations',async t=>{
  const h=await fixture(t),scenario=qa.SCENARIOS.find(value=>value.id==='support');h.runner.start(scenario);h.advance(1000000);
  const result=h.completed.at(-1);
  assert.ok(result.sourceSampleCount<400);
  assert.equal(result.actualRenderedFrameSampleCount,2);
  assert.equal(result.maxObservedFrameGapMs,scenario.duration);
  assert.equal(result.invariants.finalInquiryCueReached,true);
  assert.equal(result.visibleTrajectoryCoverage,'incomplete-sparse-observations');
  assert.equal(result.nativeSpokenTiming,'unverified');
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

test('animation-only pause preserves ongoing source audio but clears old semantic cues on resume',async t=>{
  const h=await fixture(t),scenario=qa.SCENARIOS.find(value=>value.id==='pause');h.runner.start(scenario);
  for(let at=50;at<scenario.pauseAt;at+=50)h.advance(at);
  const before=h.runner.snapshot();assert.equal(before.face.state,'speaking');assert.ok(before.controller.cue);
  h.advance(scenario.pauseAt);
  const paused=h.runner.snapshot();assert.equal(paused.face.state,'speaking');assert.equal(paused.face.paused,true);assert.equal(paused.outputRms,0);assert.equal(paused.face.mouthEnergy,0);assert.equal(paused.controller.cue,null);assert.equal(paused.face.expression,null);
  h.advance(scenario.resumeAt-50);
  const during=h.runner.snapshot();assert.equal(during.face.paused,true);assert.equal(during.face.mouthEnergy,0);assert.ok(during.sourcePlaybackSamples>paused.sourcePlaybackSamples,'native source timeline must continue while only the animation is paused');
  h.advance(scenario.resumeAt);
  const resumed=h.runner.snapshot();assert.equal(resumed.face.paused,false);assert.equal(resumed.face.state,'speaking');assert.ok(resumed.outputRms>0);assert.equal(resumed.face.speechSignal.valid,true);assert.equal(resumed.face.speechSignal.amplitude,resumed.outputRms);assert.equal(resumed.controller.cue,null);assert.equal(resumed.face.expression,null);assert.equal(resumed.controller.currentPhrase,-1);
  // The first real source sample starts a 40 ms visual attack; inspect the
  // displayed rig after time has advanced instead of asserting a fake instant.
  h.advance(scenario.resumeAt+50);assert.ok(h.runner.snapshot().face.speechEnergy>0);assert.equal(h.runner.snapshot().face.speechVisual.rippleActive,true);
  h.advance(scenario.resumeAt+1000);
  assert.equal(h.runner.snapshot().controller.cue,null,'resume must not reconstruct semantic cues from the canceled answer');
  h.advance(scenario.duration);assert.equal(h.completed.at(-1).invariants.finalInquiryCueReached,null);
});

test('the explicit QA script stays inert on the ordinary shopper route',t=>{
  const dom=new JSDOM('<!doctype html><body><div id="avatar"></div></body>',{url:'https://sandbox.example/concierge-sandbox.html',runScripts:'outside-only'}),win=dom.window;let created=0,requests=0;win.BritesConciergeAvatar={create(){created++;throw Error('Do not initialize fixture here');}};win.fetch=()=>{requests++;};win.eval(fs.readFileSync(require.resolve('../../concierge-expression-qa.js'),'utf8'));t.after(()=>win.close());assert.equal(created,0);assert.equal(requests,0);assert.equal(win.document.querySelector('#avatar').childElementCount,0);
});
