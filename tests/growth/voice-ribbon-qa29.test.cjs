'use strict';
// The production planner and SVG rig consume explicit authored spectra. These
// CPU/DOM checks do not claim captured audio, browser GPU frames or human trust.
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs');
const {JSDOM}=require('jsdom');
const qa=require('../../concierge-expression-qa.js'),expression=require('../../brites-concierge-expression.js'),avatarFactory=require('../../brites-concierge-avatar.js');
const scenario=id=>qa.SCENARIOS.find(value=>value.id===id);
const zero=()=>({amplitude:0,bands:[0,0,0,0,0,0],brightness:0,valid:false});

async function fixture(t){
  const dom=new JSDOM('<main id="mount"></main>',{url:'https://sandbox.example/concierge-expression-qa.html',pretendToBeVisual:true}),win=dom.window;let clock=0;
  Object.defineProperty(win.performance,'now',{value:()=>clock});win.matchMedia=()=>({matches:false,addEventListener(){},removeEventListener(){}});
  const guide=avatarFactory.create({container:win.document.getElementById('mount'),visible:true,greetingOnOpen:false,loadScene:async()=>{throw Error('Synthetic disabled WebGL');}});await guide.ready;
  const completed=[],runner=qa.createRunner({expressionFactory:expression,avatar:guide,now:()=>clock,onComplete:value=>completed.push(value)});
  t.after(()=>{runner.destroy();guide.destroy();win.close();});
  return {guide,runner,win,completed,setClock(value){clock=value;},advance(sourceMs,observationMs=sourceMs){clock=observationMs;return runner.update(sourceMs);},
    settle(ms=240){for(let at=0;at<ms;at+=16){clock+=Math.min(16,ms-at);guide.lookAt(0,0,false);}return runner.observe();},
    hold(id,phaseId){runner.start(scenario(id));const at=qa.inspectionTarget(scenario(id),phaseId,expression);assert.notEqual(at,null);return runner.inspectAt(at,phaseId);},
    run(id,cadence=50){const selected=scenario(id);runner.start(selected);for(let at=cadence;at<selected.duration;at+=cadence){clock=at;runner.update(at);}clock=selected.duration;runner.update(selected.duration);return completed.at(-1);}};
}
function assertQuiet(snapshot){
  assert.deepEqual(snapshot.authoredOutputSignal,zero());assert.deepEqual(snapshot.face.speechSignal,zero());
  assert.equal(snapshot.face.speechVisual.rippleActive,false);assert.equal(snapshot.face.speechVisual.amplitude,0);
  assert.equal(snapshot.face.curveClosed,true);assert.equal(snapshot.face.openingEllipsePresent,false);assert.equal(snapshot.face.curveTag,'path');
  assert.equal(snapshot.face.facePose.mouthOpen,0);assert.ok(snapshot.face.ripplePaths.every(value=>Number(value.opacity)===0));assert.ok(snapshot.face.bandPaths.every(value=>Number(value.opacity)===0));
}

test('authored signal has six bounded bins and real zero fixtures never invent spectra',()=>{
  const low=qa.authoredSignal(0,.6),high=qa.authoredSignal(1800,.6);
  assert.equal(low.amplitude,high.amplitude);assert.notDeepEqual(low.bands,high.bands);assert.ok(low.brightness<high.brightness);
  for(const at of [0,600,1200,1800,2700])for(const level of [.03,.2,.75,1,4]){const value=qa.authoredSignal(at,level);assert.equal(value.valid,true);assert.equal(value.bands.length,6);assert.ok([value.amplitude,value.brightness,...value.bands].every(number=>Number.isFinite(number)&&number>=0&&number<=1));}
  for(const [at,level] of [[0,0],[100,.015],[200,-1],[300,NaN],[Infinity,.5],[undefined,.8]])assert.deepEqual(qa.authoredSignal(at,level),zero());
});

test('held speaking samples expose a closed curve and changing actual spectrum contours at equal amplitude',async t=>{
  const h=await fixture(t);h.hold('generation','speaking');const initial=h.runner.snapshot();
  assert.equal(initial.authoredOutputSignal.valid,true);assert.equal(initial.face.state,'speaking');assert.equal(initial.face.curveClosed,true);assert.equal(initial.face.openingEllipsePresent,false);
  const middle=h.settle();assert.equal(middle.clockMs,1200);assert.equal(middle.face.speechVisual.rippleActive,true);assert.ok(middle.face.speechEnergy>0);assert.equal(middle.face.facePose.mouthOpen,0);assert.equal(middle.face.bandPaths.length,6);assert.equal(middle.face.ripplePaths.length,2);assert.equal(h.guide.element.style.getPropertyValue('--brites-eye-color'),'#70d8f1');assert.ok(middle.face.facePose.eyeOpen>=1);
  const sourceCount=middle.sourceSampleCount;h.runner.inspectAt(1800,'spectrum-high');const high=h.settle();
  assert.equal(high.authoredOutputSignal.amplitude,middle.authoredOutputSignal.amplitude);assert.notDeepEqual(high.authoredOutputSignal.bands,middle.authoredOutputSignal.bands);assert.notDeepEqual(high.face.bandPaths,middle.face.bandPaths);assert.ok(high.face.speechVisual.brightness>middle.face.speechVisual.brightness);assert.equal(high.face.facePose.mouthOpen,0);assert.ok(high.sourceSampleCount>sourceCount);
  h.runner.start(scenario('generation'));h.runner.inspectAt(1300,'quiet');assertQuiet(h.runner.snapshot());
});

test('held final questions preserve semantic targets and show independent eased brow and eye geometry',async t=>{
  const h=await fixture(t),features=[];
  for(const id of ['phrases','support','repair','mixed','uncertain','negation','generation']){
    const first=h.hold(id,'speaking');h.settle();const beginning=h.runner.snapshot();
    const target=qa.inspectionTarget(scenario(id),'inquiry',expression);assert.ok(target>first.clockMs);h.runner.inspectAt(target,'inquiry');const question=h.settle();
    assert.equal(question.controller.cue.kind,'inquiry',id);assert.equal(question.face.expression.kind,'inquiry',id);assert.equal(question.face.curveClosed,true);assert.equal(question.face.facePose.mouthOpen,0);assert.notEqual(question.face.features.brows,beginning.face.features.brows,id);features.push(question.face.features.eyes);
  }
  assert.ok(new Set(features).size>1);assert.equal(qa.inspectionTarget(scenario('pause'),'inquiry',expression),null);assert.equal(qa.inspectionTarget(scenario('listen'),'inquiry',expression),null);
});

test('explicit product focus and selection preserve the current signal without re-forwarding audio',async t=>{
  const h=await fixture(t);h.hold('generation','product-focus');const before=h.settle(),target=before.face.speechSignal,cue=before.controller.cue;
  assert.equal(before.face.speechVisual.rippleActive,true);const sourceCount=before.sourceSampleCount,sourcePlayback=before.sourcePlaybackSamples,observed=before.actualRenderedFrameSampleCount;
  for(const [type,choice] of [['selection','b'],['focus','a'],['focus',null]]){
    const next=h.runner.productAction(type,choice);assert.equal(next.clockMs,before.clockMs);assert.equal(next.face.state,'speaking');assert.deepEqual(next.face.speechSignal,target);assert.deepEqual(next.controller.cue,cue);assert.equal(next.face.speechVisual.rippleActive,true);assert.equal(next.face.facePose.mouthOpen,0);assert.equal(next.sourceSampleCount,sourceCount);assert.equal(next.sourcePlaybackSamples,sourcePlayback);
  }
  const after=h.runner.snapshot();assert.equal(after.actualRenderedFrameSampleCount,observed+3);assert.equal(after.product.selectedChoice,'b');assert.equal(after.face.productFocus,null);assert.equal(after.providerCalls,0);assert.equal(after.cartWrites,0);assert.equal(h.runner.productAction('purchase','b'),false);
});

test('the generation tape retains voice across selection, clears the authored quiet window and reaches its question',async t=>{
  const h=await fixture(t),item=scenario('generation');h.runner.start(item);for(let at=50;at<=6200;at+=50)h.advance(at);const quiet=h.runner.snapshot();assert.equal(quiet.product.selectedChoice,'b');assertQuiet(quiet);
  for(let at=6250;at<=item.duration;at+=50)h.advance(at);const result=h.completed.at(-1);
  assert.deepEqual(result.failedInvariants,[]);assert.equal(result.invariants.productSelectionKeepsVoiceSignal,true);assert.ok(result.selectionSourcePositiveSamples>0);assert.ok(result.selectionObservedRippleSamples>0);assert.ok(result.selectionObservedRippleSamples<=result.selectionSourcePositiveSamples);assert.ok(result.distinctObservedSignalTargets>3);assert.ok(result.distinctObservedDisplayedSpectra>3);assert.ok(result.distinctObservedRipplePaths>3);assert.equal(result.invariants.finalInquiryCueReached,true);assert.equal(result.invariants.closedCurveWithoutOpeningEllipse,true);assert.deepEqual(result.sourceEventCoverage,qa.tapeFor(item).map(value=>({at:value.at,type:value.type})));assert.equal(result.gpuAppearance,'unverified');
});

test('held animation-only pause is directly observable while source continuation and canceled cues remain separate',async t=>{
  const h=await fixture(t);h.hold('pause','paused');const paused=h.runner.snapshot();assert.equal(paused.inspectionHeld,true);assert.equal(paused.clockMs,4100);assert.equal(paused.face.paused,true);assert.equal(paused.face.state,'speaking');assert.equal(paused.controller.cue,null);assert.equal(paused.face.expression,null);assertQuiet(paused);
  h.setClock(10000);h.runner.step();h.runner.update(11000);const unchanged=h.runner.snapshot();assert.equal(unchanged.clockMs,4100);assert.equal(unchanged.sourceSampleCount,paused.sourceSampleCount);assert.equal(unchanged.sourcePlaybackSamples,paused.sourcePlaybackSamples);
  h.runner.observe();const observed=h.runner.snapshot();assert.equal(observed.actualRenderedFrameSampleCount,paused.actualRenderedFrameSampleCount+1);assert.equal(observed.sourceSampleCount,paused.sourceSampleCount);assertQuiet(observed);
  assert.ok(h.runner.continueInspection());h.advance(6200,12100);const resumed=h.runner.snapshot();assert.equal(resumed.face.paused,false);assert.equal(resumed.face.state,'speaking');assert.equal(resumed.face.speechSignal.valid,true);assert.ok(resumed.face.speechSignal.amplitude>0);assert.ok(resumed.sourcePlaybackSamples>paused.sourcePlaybackSamples);assert.equal(resumed.controller.cue,null);assert.equal(resumed.face.expression,null);
  const settled=h.settle(160);assert.equal(settled.clockMs,6200);assert.equal(settled.face.speechVisual.rippleActive,true);assert.ok(settled.face.speechEnergy>0);assert.equal(settled.controller.currentPhrase,-1);assert.equal(settled.face.facePose.mouthOpen,0);h.advance(scenario('pause').duration,20000);assert.equal(h.completed.at(-1).visibleTrajectoryCoverage,'authored-phase-inspection-no-natural-cadence-claim');
});

test('held hidden phase stops the source and returning guide stays quiet without reconstructing the answer',async t=>{
  const h=await fixture(t);h.hold('hidden','hidden');const hidden=h.runner.snapshot();assert.equal(hidden.clockMs,4100);assert.equal(hidden.face.visible,false);assert.equal(hidden.face.state,'listening');assert.equal(hidden.controller.cue,null);assertQuiet(hidden);
  h.setClock(12000);h.runner.step();h.runner.observe();assert.equal(h.runner.snapshot().sourcePlaybackSamples,hidden.sourcePlaybackSamples);assert.equal(h.runner.snapshot().clockMs,4100);assert.equal(h.guide.element.hidden,true);
  h.runner.inspectAt(7200,'shown');const shown=h.runner.snapshot();assert.equal(shown.face.visible,true);assert.equal(shown.face.paused,false);assert.equal(shown.face.state,'listening');assert.equal(shown.sourcePlaybackSamples,hidden.sourcePlaybackSamples);assert.equal(shown.controller.cue,null);assert.equal(shown.face.expression,null);assertQuiet(shown);assert.equal(h.guide.element.hidden,false);
});

test('listening, interruption, unavailable analysis and reduced motion keep a closed quiet curve',async t=>{
  const h=await fixture(t);h.hold('listen-support','listening-support');const supportive=h.settle();assert.equal(supportive.controller.cue.kind,'support');assert.equal(supportive.face.expression.kind,'support');assertQuiet(supportive);
  for(const id of ['analysis','reduced','silence']){const result=h.run(id);assert.deepEqual(result.failedInvariants,[]);assertQuiet(h.runner.snapshot());assert.equal(result.gpuAppearance,'unverified');}
  h.hold('generation','product-selection');h.settle();assert.equal(h.runner.snapshot().face.speechVisual.rippleActive,true);h.runner.interrupt();const interrupted=h.runner.snapshot();assert.equal(interrupted.face.state,'listening');assert.equal(interrupted.controller.cue.kind,'attentive');assertQuiet(interrupted);
});

test('sparse source updates preserve authored events and cue coverage while actual pose observations remain sparse',async t=>{
  const dense=await fixture(t),sparse=await fixture(t),normal=dense.run('generation',50),slow=sparse.run('generation',1000);
  assert.deepEqual(slow.sourceEventCoverage,normal.sourceEventCoverage);assert.deepEqual(slow.sourcePhraseIndices,normal.sourcePhraseIndices);assert.deepEqual(slow.distinctCueKinds,normal.distinctCueKinds);assert.equal(slow.sourceSampleCount,normal.sourceSampleCount);assert.equal(slow.sourcePlaybackSamples,normal.sourcePlaybackSamples);assert.equal(slow.invariants.finalInquiryCueReached,true);assert.equal(slow.invariants.productSelectionKeepsVoiceSignal,true);assert.ok(slow.actualRenderedFrameSampleCount<normal.actualRenderedFrameSampleCount/5);assert.equal(slow.maxObservedSourceTimeGapMs,1000);assert.equal(slow.maxObservationTimeGapMs,1000);assert.equal(slow.visibleTrajectoryCoverage,'incomplete-sparse-observations');assert.match(slow.actualRenderedFrameSampleMeaning,/not a GPU frame count/);
});

test('inspection controls are explicit authored UI, and the script makes no ordinary-shopper initialization or requests',t=>{
  const html=fs.readFileSync(require.resolve('../../concierge-expression-qa.html'),'utf8'),dom=new JSDOM(html,{url:'https://sandbox.example/concierge-sandbox.html',runScripts:'outside-only'}),win=dom.window;let requests=0,created=0;
  for(const id of ['inspection-phase','inspect-phase','continue-inspection','observe-inspection','focus-sample','select-sample','clear-sample'])assert.ok(win.document.getElementById(id),id);
  assert.ok(win.document.querySelector('label[for="inspection-phase"]'));assert.match(win.document.body.textContent,/authored|Authored/);assert.match(win.document.body.textContent,/no intervening rendered frames/);assert.match(win.document.body.textContent,/not captured audio/);
  win.BritesConciergeAvatar={create(){created++;throw Error('Should remain inert');}};win.fetch=()=>{requests++;};win.eval(fs.readFileSync(require.resolve('../../concierge-expression-qa.js'),'utf8'));t.after(()=>win.close());assert.equal(requests,0);assert.equal(created,0);assert.equal(win.document.getElementById('avatar').childElementCount,0);
});
