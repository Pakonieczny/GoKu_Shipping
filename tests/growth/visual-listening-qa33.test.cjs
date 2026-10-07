'use strict';

// Explicit authored incoming fixtures use the real SVG controller. These are
// synthetic source samples, never microphone, GPU or physical voice evidence.
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs');
const {JSDOM}=require('jsdom');
const qa=require('../../concierge-expression-qa.js'),expression=require('../../brites-concierge-expression.js'),avatars=require('../../brites-concierge-avatar.js');
const scenario=id=>qa.SCENARIOS.find(value=>value.id===id),zero=()=>qa.authoredSignal(0,0);

async function fixture(t){
  const dom=new JSDOM('<main></main>',{url:'https://sandbox.example/',pretendToBeVisual:true}),win=dom.window;
  let clock=0,job=0,intersection;const jobs=new Map();Object.defineProperty(win.performance,'now',{value:()=>clock});
  win.matchMedia=()=>({matches:false,addEventListener(){},removeEventListener(){}});win.requestAnimationFrame=()=>0;win.cancelAnimationFrame=()=>{};
  win.IntersectionObserver=class{constructor(callback){intersection=callback;}observe(){}disconnect(){}};
  win.setTimeout=(callback,delay)=>{jobs.set(++job,{callback,at:clock+delay});return job;};win.clearTimeout=id=>jobs.delete(id);
  const guide=avatars.create({container:win.document.querySelector('main'),visible:true,greetingOnOpen:false,loadScene:async()=>{throw Error('Synthetic no WebGL');}});await guide.ready;
  const runner=qa.createRunner({expressionFactory:expression,avatar:guide,now:()=>clock,setTimer:win.setTimeout,clearTimer:win.clearTimeout});
  t.after(()=>{runner.destroy();guide.destroy();win.close();});
  return {guide,runner,hold(id,phase){runner.start(scenario(id));return runner.inspectAt(qa.inspectionTarget(scenario(id),phase,expression),phase);},
    real(ms){for(let at=0;at<ms;at+=16){clock+=16;for(const [id,value] of [...jobs])if(value.at<=clock){jobs.delete(id);value.callback();}guide.lookAt(0,0,false);}return runner.snapshot();},intersect(value){intersection([{isIntersecting:value,intersectionRatio:value?1:0}]);},jobs};
}
function quiet(value){assert.deepEqual(value.authoredOutputSignal,zero());assert.deepEqual(value.face.speechSignal,zero());assert.equal(value.face.speechEnergy,0);assert.equal(value.face.facePose.mouthOpen,0);assert.equal(value.face.speechVisual.rippleActive,false);}

test('incoming authored signal reaches attentive eyes and brows independently of the closed quiet mouth',async t=>{
  const h=await fixture(t);h.hold('listen','listening');h.real(800);const before=h.runner.snapshot();assert.deepEqual(before.authoredInputSignal,zero());assert.equal(before.face.inputEnergy,0);quiet(before);
  h.hold('listen','listening-input');const initial=h.runner.snapshot();assert.equal(initial.authoredInputSignal.amplitude,.43);assert.equal(initial.authoredInputSignal.valid,true);assert.equal(initial.heldSyntheticInput.active,true);
  const after=h.real(1200);assert.deepEqual(after.face.inputSignal,after.authoredInputSignal);assert.ok(after.face.inputEnergy>.42);assert.ok(after.face.facePose.faceBrowLift>before.face.facePose.faceBrowLift+.04);assert.ok(after.face.facePose.eyeRoundness>before.face.facePose.eyeRoundness+.03);assert.equal(after.face.expression.kind,'attentive');quiet(after);
});

test('held incoming display copies are labeled and advance no source clock, phrase, packet or recorded observations',async t=>{
  const h=await fixture(t);h.hold('listen-support','listening-support');const before=h.runner.snapshot();assert.equal(before.controller.cue.kind,'support');assert.equal(before.face.expression.kind,'support');
  const after=h.real(1500);assert.ok(after.heldSyntheticInput.applicationsAfterSourceSample>=10);assert.match(after.heldSyntheticInput.meaning,/Same explicitly held synthetic/);assert.match(after.heldSyntheticInput.meaning,/no additional source packets/);
  for(const key of ['clockMs','sourceSampleCount','sourcePlaybackSamples','actualRenderedFrameSampleCount','inputRms','outputRms'])assert.equal(after[key],before[key],key+' stays at the actual held source observation');
  assert.deepEqual(after.controller,before.controller);assert.deepEqual(after.authoredInputSignal,before.authoredInputSignal);assert.deepEqual(after.authoredOutputSignal,before.authoredOutputSignal);assert.ok(after.face.inputEnergy>.17);assert.equal(after.providerCalls,0);assert.equal(after.microphoneRequests,0);quiet(after);
});

test('continuation and every authored stop clear held incoming display and prevent replay',async t=>{
  const h=await fixture(t);
  for(const stop of [()=>h.runner.pause(true),()=>h.runner.interrupt(),()=>h.runner.reset(),()=>h.runner.destroy(),()=>h.runner.continueInspection()]){
    h.hold('listen','listening-input');h.real(500);assert.ok(h.runner.snapshot().face.inputEnergy>.4);stop();const stopped=h.runner.snapshot();assert.equal(stopped.heldSyntheticInput.active,false);assert.deepEqual(stopped.face.inputSignal,zero());assert.equal(stopped.face.inputEnergy,0);h.real(1000);assert.deepEqual(h.runner.snapshot().face.inputSignal,zero(),'old held sample cannot reappear');quiet(h.runner.snapshot());
  }
});

test('speaking, quiet listening, unavailable analysis and reduced motion never receive an active incoming fixture',async t=>{
  const h=await fixture(t);
  for(const [id,phase] of [['phrases','speaking'],['listen','listening'],['analysis','speaking'],['silence','speaking'],['reduced','speaking']]){h.hold(id,phase);h.real(700);const value=h.runner.snapshot();assert.deepEqual(value.authoredInputSignal,zero());assert.deepEqual(value.face.inputSignal,zero());assert.equal(value.face.inputEnergy,0);assert.equal(value.heldSyntheticInput.active,false);}
  h.hold('listen','listening-input');h.real(400);h.runner.start(scenario('phrases'));h.real(700);assert.deepEqual(h.runner.snapshot().face.inputSignal,zero(),'changing scenarios clears the held input source');
});

test('the public QA distinguishes authored incoming visual inspection from captured media',()=>{
  const html=fs.readFileSync(require.resolve('../../concierge-expression-qa.html'),'utf8'),source=fs.readFileSync(require.resolve('../../concierge-expression-qa.js'),'utf8');
  assert.ok(qa.INSPECTION_PHASES.some(value=>value.id==='listening-input'&&/synthetic incoming energy/.test(value.label)));assert.match(html,/not microphone audio or additional source packets/);assert.match(source,/authoredInputSignal:snapshot\.authoredInputSignal/);assert.match(source,/heldSyntheticInput:snapshot\.heldSyntheticInput/);assert.doesNotMatch(source,/getUserMedia|new RTCPeerConnection|\/api\/growth\/voice/);
});

test('an explicitly held output survives return from offscreen without inventing new source observations',async t=>{
  const h=await fixture(t);h.hold('generation','speaking');h.real(400);const before=h.runner.snapshot();
  assert.equal(before.authoredOutputSignal.amplitude,.12);assert.equal(before.face.speechVisual.rippleActive,true);assert.equal(before.heldSyntheticOutput.active,true);
  h.intersect(false);h.real(400);const hidden=h.runner.snapshot();assert.deepEqual(hidden.face.speechSignal,zero());assert.equal(hidden.face.speechVisual.rippleActive,false);
  h.intersect(true);const after=h.real(500);assert.deepEqual(after.face.speechSignal,after.authoredOutputSignal);assert.equal(after.face.speechVisual.rippleActive,true);assert.ok(after.face.speechEnergy>.11);
  for(const key of ['clockMs','sourceSampleCount','sourcePlaybackSamples','actualRenderedFrameSampleCount'])assert.equal(after[key],before[key],key+' never advances on held repaint');
  assert.deepEqual(after.controller,before.controller);assert.match(after.heldSyntheticOutput.meaning,/no additional source packets/);assert.ok(after.heldSyntheticOutput.reapplicationsAfterSourceSample>before.heldSyntheticOutput.reapplicationsAfterSourceSample);assert.equal(after.face.facePose.mouthOpen,0);
});

test('held output remains quiet at all authored stop boundaries and never leaks into incoming listening',async t=>{
  const h=await fixture(t);
  for(const stop of [()=>h.runner.pause(true),()=>h.runner.interrupt(),()=>h.runner.reset(),()=>h.runner.destroy(),()=>h.runner.continueInspection()]){
    h.hold('generation','speaking');h.real(300);assert.equal(h.runner.snapshot().face.speechVisual.rippleActive,true);stop();assert.equal(h.runner.snapshot().heldSyntheticOutput.active,false);
    if(h.runner.snapshot().inspectionHeld===false&&h.runner.snapshot().running&&h.runner.snapshot().face.state==='speaking'){const count=h.runner.snapshot().heldSyntheticOutput.reapplicationsAfterSourceSample;h.real(700);assert.equal(h.runner.snapshot().heldSyntheticOutput.reapplicationsAfterSourceSample,count,'continuation disables the display-only hold');}
    else{h.real(700);quiet(h.runner.snapshot());}
  }
  h.hold('generation','quiet');h.real(500);quiet(h.runner.snapshot());assert.equal(h.runner.snapshot().heldSyntheticOutput.active,false);
  h.hold('listen','listening-input');h.real(700);quiet(h.runner.snapshot());assert.equal(h.runner.snapshot().heldSyntheticOutput.active,false);assert.ok(h.runner.snapshot().face.inputEnergy>.4);
});

test('held output labels distinguish a repaint attempt from source packets or captured sound',()=>{
  const html=fs.readFileSync(require.resolve('../../concierge-expression-qa.html'),'utf8'),source=fs.readFileSync(require.resolve('../../concierge-expression-qa.js'),'utf8');
  assert.match(html,/positive speaking phases reapply the same authored sample/);assert.match(html,/advance no source clock or recorded frame observations/);assert.match(source,/heldSyntheticOutput:snapshot\.heldSyntheticOutput/);
});
