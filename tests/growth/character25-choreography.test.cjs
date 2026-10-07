'use strict';
// Actual controller and CPU pose behaviour; no GPU, audio device or rendered
// appearance is certified by these checks.
const test = require('node:test'), assert = require('node:assert/strict');
const {JSDOM} = require('jsdom');
const avatar = require('../../brites-concierge-avatar.js');

async function fixture(t, {reduced = false, fallback = false} = {}) {
  const dom = new JSDOM('<div id="mount"></div>', {url: 'https://sandbox.example/', pretendToBeVisual: true});
  const win = dom.window; let milliseconds = 1000, hidden = false, frames, observer;
  Object.defineProperty(win.performance, 'now', {value: () => milliseconds});
  Object.defineProperty(win.document, 'hidden', {get: () => hidden});
  win.matchMedia = () => ({matches: reduced, addEventListener() {}, removeEventListener() {}});
  win.IntersectionObserver = class {constructor(callback) {observer = callback;} observe() {} disconnect() {}};
  const engine = {setMotion() {}, render() {}, invalidate() {}, destroy() {}, snapshot() {return {};}};
  const guide = avatar.create({container: win.document.getElementById('mount'), visible: true, greetingOnOpen: false,
    loadScene: async () => {if (fallback) throw Error('Synthetic WebGL disabled'); return {createAvatarScene(config) {frames = config.onFrame; return engine;}};}});
  await guide.ready;
  t.after(() => {guide.destroy(); win.close();});
  return {guide, win, pose: () => frames(milliseconds / 1000), advance(ms) {milliseconds += ms;}, hide(value) {hidden = value; win.document.dispatchEvent(new win.Event('visibilitychange'));}, intersect(value) {observer([{isIntersecting: value}]);}};
}

test('product gaze deliberately leads head, followed by selected-side presentation', async t => {
  const f = await fixture(t); f.guide.setState('listening');
  assert.equal(f.guide.focusProduct({x: .9, y: -.4}), true);
  const initial = f.pose(); f.advance(90); const early = f.pose(); f.advance(350); const presented = f.pose();
  assert.ok(early.gazeX > initial.gazeX + .025, 'eyes begin moving before the head');
  assert.equal(early.headYaw, initial.headYaw, 'head anticipation is held for 100 ms');
  assert.ok(presented.headYaw > early.headYaw + .06);
  assert.ok(presented.present > .5);
  assert.ok(presented.armLiftRight > presented.armLiftLeft + .15);
  assert.ok(presented.armReach > 0);
});

test('left product uses left hand and held target outranks cursor gaze', async t => {
  const f = await fixture(t); f.guide.setState('listening');
  f.guide.focusProduct({x: -.8, y: .3}); f.advance(440); f.guide.lookAt(1, -1);
  const p = f.pose(); assert.ok(p.gazeX < -.07); assert.ok(p.armLiftLeft > p.armLiftRight);
  f.guide.clearFocus(); assert.equal(f.guide.snapshot().productFocus, null); assert.ok(f.pose().gazeX > 0);
});

test('duplicate pointer target does not restart anticipation or finite hand gesture', async t => {
  const f = await fixture(t); f.guide.focusProduct({x: .8, y: .4}); const id = f.guide.snapshot().mannerism.id;
  f.advance(300); f.guide.focusProduct({x: .81, y: .39});
  assert.equal(f.guide.snapshot().mannerism.id, id); assert.ok(f.guide.snapshot().mannerism.elapsed >= .299);
  f.advance(1300); assert.equal(f.pose().present, 0); assert.equal(f.pose().armReach, 0);
});

test('speech phrase beat starts on measured energy after a rest and settles under held audio', async t => {
  const f = await fixture(t); f.guide.setState('speaking'); f.advance(1200);
  f.guide.setLevel(.7); f.advance(280); assert.ok(f.pose().phraseGesture > .5);
  f.advance(900); f.guide.setLevel(.7); assert.equal(f.pose().phraseGesture, 0, 'held noise is not a recurring wave');
  f.guide.setLevel(.01); f.advance(200); f.guide.setLevel(.8); f.advance(280); assert.ok(f.pose().phraseGesture > .6);
  f.guide.setLevel(0); assert.equal(f.pose().phraseGesture, 0);
});

test('quiet grief context retains attention but suppresses presentation and celebration', async t => {
  const f = await fixture(t); f.guide.setEmotion('calm'); f.guide.focusProduct({x: .8, y: .2}); f.advance(440);
  assert.ok(f.pose().gazeX > 0); assert.equal(f.pose().offer, 0); assert.equal(f.pose().armReach, 0);
  f.guide.cue('confirm'); assert.equal(f.guide.snapshot().mannerism.name, 'reassure');
});

test('hidden, offscreen and paused cancel target and reject subsequent product cues', async t => {
  const f = await fixture(t);
  for (const [stop, start] of [[() => f.guide.setPaused(true), () => f.guide.setPaused(false)], [() => f.hide(true), () => f.hide(false)], [() => f.intersect(false), () => f.intersect(true)]]) {
    f.guide.focusProduct({x: 1, y: 1}); stop();
    assert.equal(f.guide.snapshot().productFocus, null); assert.equal(f.guide.snapshot().mannerism.active, false);
    assert.equal(f.guide.focusProduct({x: 1, y: 1}), false); assert.equal(f.guide.cue('explain'), false);
    f.guide.setLevel(1); assert.equal(f.pose().speechEnergy, 0); start();
    assert.equal(f.guide.snapshot().productFocus, null, 'resume never replays a retired target');
  }
});

test('reduced motion offers static directional attention without timed gesture', async t => {
  const f = await fixture(t, {reduced: true}); f.guide.focusProduct({x: .8, y: -.2}); const p = f.pose();
  assert.ok(p.gazeX > 0); assert.equal(p.present, 0); assert.equal(p.armReach, 0); assert.equal(f.guide.cue('explain'), false);
  f.advance(5000); assert.deepEqual(f.pose(), p);
});

test('destroy and invalid targets cannot mutate or replay the character', async t => {
  const f = await fixture(t); assert.equal(f.guide.focusProduct({x: NaN, y: 0}), false);
  assert.equal(f.guide.focusProduct({x: 0, y: Infinity}), false); assert.equal(f.guide.cue('execute-checkout'), false);
  f.guide.destroy(); assert.equal(f.guide.focusProduct({x: 1, y: 1}), false); assert.equal(f.guide.cue('greet'), false);
});

test('measured closed-curve fallback responds to current sound with one bounded attack and no invented spectrum',async t=>{
  const f=await fixture(t,{fallback:true});f.guide.setState('speaking');f.advance(1200);f.guide.setLevel(0);
  const quiet=f.guide.element.style.getPropertyValue('--brites-speech-scale'),mouth=f.guide.element.querySelector('.brites-avatar__speech-mouth'),base=f.guide.element.querySelector('.brites-avatar__smile-signal');
  const quietBands=[...f.guide.element.querySelectorAll('.brites-avatar__speech-band')].map(node=>node.getAttribute('d'));
  const current={amplitude:.9,bands:[.02,.07,.18,.6,.15,.05],brightness:.45,valid:true};f.guide.setSpeechSignal(current);
  assert.equal(f.guide.snapshot().speechVisual.amplitude,0,'same-clock sample cannot bypass the authored attack');
  f.advance(40);f.guide.setSpeechSignal(current);
  const shown=f.guide.snapshot().speechVisual;assert.ok(shown.amplitude>.55&&shown.amplitude<.59);assert.equal(shown.curveClosed,true);assert.equal(shown.rippleActive,true);
  assert.notEqual(f.guide.element.style.getPropertyValue('--brites-speech-scale'),quiet);assert.ok(Number(mouth.getAttribute('opacity'))>0);
  assert.equal(f.guide.snapshot().facePose.mouthOpen,0);assert.equal(mouth.getAttribute('fill'),'none');assert.equal(mouth.getAttribute('d'),base.getAttribute('d'),'emission overlays the same closed-lip expression curve');
  const bands=[...f.guide.element.querySelectorAll('.brites-avatar__speech-band')];assert.equal(bands.length,6);assert.notEqual(bands[3].getAttribute('d'),quietBands[3],'the current measured band changes its own curved segment');assert.ok(Number(bands[3].getAttribute('opacity'))>0);
  assert.deepEqual(f.guide.snapshot().speechSignal.bands,current.bands,'six-band target comes from the caller rather than a wall clock');
  f.guide.setSpeechSignal(null);assert.equal(f.guide.snapshot().speechVisual.amplitude,0);assert.equal(mouth.getAttribute('opacity'),'0');assert.ok(bands.every(node=>node.getAttribute('opacity')==='0'));
});

 test('2-D fallback product eye and palm visibly receive a finite presentation target', async t => {
  const f = await fixture(t, {fallback: true});
  f.guide.focusProduct({x: -.9, y: .3});
  assert.equal(f.guide.snapshot().mode, 'fallback');
  assert.ok(parseFloat(f.guide.element.style.getPropertyValue('--brites-gaze-x')) < -10);
  assert.ok(parseFloat(f.guide.element.style.getPropertyValue('--brites-point-left')) > 20);
  f.guide.clearFocus(); assert.equal(f.guide.element.dataset.productFocused, 'false');
  assert.ok(Math.abs(parseFloat(f.guide.element.style.getPropertyValue('--brites-gaze-x'))) < 3);
 });
