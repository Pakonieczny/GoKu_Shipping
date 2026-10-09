'use strict';

// Production pose/controller checks with a deterministic DOM clock. These do
// not certify GPU pixels, human preference, microphone input or audio latency.
const test = require('node:test');
const assert = require('node:assert/strict');
const {JSDOM} = require('jsdom');
const avatar = require('../../brites-concierge-avatar.js');

test('waiting and ordinary spoken inquiry keep balanced open eyes and a relaxed closed smile', () => {
  const waiting = avatar.poseFor({state: 'thinking', emotion: 'curious'});
  assert.equal(waiting.faceExpression, 'curious');
  assert.ok(waiting.eyeScaleX >= 1);
  assert.ok(waiting.eyeDeformation > -.04);
  assert.ok(waiting.eyeAsymmetry < .03);
  assert.ok(waiting.faceBrowTilt < .2);
  assert.ok(waiting.mouthTension < .15);
  assert.equal(waiting.mouthSkew, 0);
  assert.ok(waiting.mouthCurve > 0);
  assert.equal(waiting.mouthOpen, 0);
  assert.ok(Math.abs(waiting.headRoll) < .02);
  assert.ok(waiting.armLift < .1);
  const question = avatar.poseFor({state: 'speaking', expression: {kind: 'inquiry', intensity: 1}});
  assert.equal(question.faceBrowTilt, avatar.FACE_EXPRESSIONS.curious.faceBrowTilt);
  assert.ok(question.faceBrowLift < waiting.faceBrowLift && question.faceSignal > waiting.faceSignal, 'spoken inquiry retains paired shape contrast without confusion');
  assert.equal(question.eyeAsymmetry, 0); assert.equal(question.mouthSkew, 0); assert.ok(question.mouthCurve > 0);
  const quiet = avatar.poseFor({state: 'thinking', emotion: 'calm'});
  assert.ok(quiet.mouthCurve > 0 && quiet.smileCurve < waiting.smileCurve);
  assert.equal(quiet.mouthSkew, 0); assert.equal(quiet.eyeAsymmetry, 0);
});

test('authored waiting attention varies softly within its request and keeps body and silent mouth still', () => {
  const cues = new Set(), shapes = new Set();
  for (let elapsed = 0; elapsed < 55; elapsed += .04) {
    const pose = avatar.poseFor({state: 'thinking', elapsed, time: elapsed + 100});
    if (pose.thinkingCue) cues.add(pose.thinkingCue);
    shapes.add([pose.faceBrowLift.toFixed(2), pose.smileCurve.toFixed(2)].join(','));
    assert.ok(pose.smileCurve >= .34 && pose.smileCurve <= .57);
    assert.ok(Math.abs(pose.mouthSkew) < .001);
    assert.ok(Math.abs(pose.eyeAsymmetry) < .03);
    assert.ok(Math.abs(pose.headRoll) < .021);
    assert.equal(pose.bodyDepth, 0); assert.equal(pose.bodyYaw, 0); assert.equal(pose.bodyRoll, 0); assert.equal(pose.bob, 0);
    assert.equal(pose.speechEnergy, 0); assert.equal(pose.mouthOpen, 0);
    for (const value of Object.values(pose)) if (typeof value === 'number') assert.ok(Number.isFinite(value));
  }
  assert.deepEqual([...cues].sort(), ['check', 'consider', 'patient', 'soften']);
  assert.ok(shapes.size >= 12, 'a wait does not hold one static geometry signature');
  const at = avatar.thinkingFor({elapsed: 2.06});
  assert.deepEqual(avatar.thinkingFor({elapsed: 2.06 + avatar.THINKING_CYCLE}), at);
  for (const elapsed of [null, NaN, Infinity, -1]) assert.equal(avatar.thinkingFor({elapsed}).active, false);
});

test('a listener question or reflection surviving into waiting cannot restore asymmetric eyes or a crooked mouth', () => {
  for (const kind of avatar.EXPRESSION_KINDS) for (const intensity of [.5, .7, 1]) {
    const pose = avatar.poseFor({state: 'thinking', elapsed: 2.06, expression: {kind, intensity}});
    assert.equal(pose.thinkingCue, 'consider', kind + ': waiting still varies alongside a listener cue');
    assert.ok(Math.abs(pose.faceBrowTilt) <= .18);
    assert.ok(Math.abs(pose.eyeAsymmetry) <= .018);
    assert.ok(Math.abs(pose.mouthSkew) <= .025);
    assert.ok(pose.smileCurve >= .31 && pose.smileCurve <= .58);
    assert.ok(pose.mouthTension <= .2);
    assert.ok(Math.abs(pose.expressionHeadRoll) <= .01);
    assert.equal(pose.speechEnergy, 0);
  }
  const spoken = avatar.poseFor({state: 'speaking', expression: {kind: 'inquiry', intensity: .7}});
  const explained = avatar.poseFor({state: 'speaking', expression: {kind: 'explain', intensity: .7}});
  assert.equal(spoken.expressionKind, 'inquiry'); assert.ok(Math.abs(spoken.faceBrowTilt) < .01); assert.equal(spoken.eyeAsymmetry, 0);
  assert.ok(spoken.faceBrowLift > explained.faceBrowLift + .07 && spoken.eyeRoundness > explained.eyeRoundness + .01, 'ordinary inquiry retains friendly geometric distinction');
});

test('cursor and exact product attention suppress authored glances while preserving facial warmth', () => {
  const elapsed = 2.06, free = avatar.poseFor({state: 'thinking', elapsed});
  assert.ok(free.gazeX > 0 && free.gazeY > 0);
  const centered = avatar.poseFor({state: 'thinking', elapsed, attentionActive: true});
  assert.equal(centered.gazeX, 0); assert.equal(centered.gazeY, 0);
  assert.equal(centered.headPitch, 0); assert.equal(centered.headRoll, .004);
  assert.equal(centered.smileCurve, free.smileCurve);
  const gaze = {x: -.7, y: .65};
  for (const extra of [{gaze}, {gaze, productFocus: {x: -.7, y: .65}}, {gaze, headGaze: gaze, attentionActive: true}]) {
    const pose = avatar.poseFor({state: 'thinking', elapsed, ...extra});
    assert.equal(pose.gazeX, gaze.x * .07); assert.equal(pose.gazeY, gaze.y * .07);
    assert.equal(pose.headPitch, -gaze.y * .09);
    assert.equal(pose.headYaw, gaze.x * .1);
  }
});

test('request-local irregular blinks close briefly; reduced motion removes all waiting trajectories', () => {
  for (const event of avatar.THINKING_BLINKS) {
    assert.ok(event.duration >= .18 && event.duration <= .22);
    assert.equal(avatar.thinkingFor({elapsed: event.at - .01}).blink, 0);
    assert.ok(avatar.thinkingFor({elapsed: event.at + event.duration / 2}).blink > .99);
    assert.equal(avatar.thinkingFor({elapsed: event.at + event.duration + .01}).blink, 0);
  }
  const rest = avatar.poseFor({state: 'thinking', reducedMotion: true, elapsed: 0, time: 1});
  for (const elapsed of [2.06, 6.2, 10.82, 17.4, 100]) {
    assert.deepEqual(avatar.poseFor({state: 'thinking', reducedMotion: true, elapsed, time: elapsed + 300}), rest);
  }
  assert.equal(rest.blink, 0); assert.equal(rest.thinkingCue, null);
  const quiet = avatar.thinkingFor({elapsed: 2.06, quiet: true}), ordinary = avatar.thinkingFor({elapsed: 2.06});
  assert.ok(quiet.intensity < ordinary.intensity);
  assert.ok(Math.abs(quiet.headRoll) < Math.abs(ordinary.headRoll));
});

async function fixture(t, fallback = false) {
  const dom = new JSDOM('<button id="shop">Shop</button><div id="mount"></div>', {url: 'https://sandbox.example/', pretendToBeVisual: true});
  const win = dom.window, doc = win.document, frames = new Map(), timers = new Map();
  let clock = 1000, hidden = false, serial = 0, intersect, sceneFrame, sceneActive = false;
  Object.defineProperty(win.performance, 'now', {value: () => clock});
  Object.defineProperty(doc, 'hidden', {get: () => hidden});
  win.matchMedia = () => ({matches: false, addEventListener() {}, removeEventListener() {}});
  win.requestAnimationFrame = fn => {frames.set(++serial, fn); return serial;};
  win.cancelAnimationFrame = id => frames.delete(id);
  win.setTimeout = (fn, delay) => {timers.set(++serial, {fn, due: clock + delay}); return serial;};
  win.clearTimeout = id => timers.delete(id);
  win.IntersectionObserver = class {constructor(fn) {intersect = fn;} observe() {} disconnect() {}};
  const engine = {setMotion(value) {sceneActive = value.active && !value.reducedMotion;}, render() {}, invalidate() {}, destroy() {}, snapshot() {return {};}};
  const guide = avatar.create({container: doc.getElementById('mount'), visible: true, greetingOnOpen: false, loadScene: async () => {
    if (fallback) throw Error('Synthetic graphics unavailable');
    return {createAvatarScene(config) {sceneFrame = config.onFrame; return engine;}};
  }});
  const box = {left: 100, top: 100, width: 320, height: 280};
  guide.element.getBoundingClientRect = () => ({...box});
  guide.element.querySelector('svg').getBoundingClientRect = () => ({...box});
  await guide.ready;
  function advance(ms, observe) {
    const end = clock + ms;
    while (clock < end) {
      clock = Math.min(end, clock + 16);
      const ready = [...frames.values()]; frames.clear(); ready.forEach(fn => fn(clock));
      for (const [id, timer] of [...timers]) if (timer.due <= clock) {timers.delete(id); timer.fn();}
      if (sceneActive) sceneFrame?.(clock / 1000);
      observe?.(guide.snapshot());
    }
  }
  t.after(() => {guide.destroy(); win.close();});
  return {guide, frames, timers, advance, hide(value) {hidden = value; doc.dispatchEvent(new win.Event('visibilitychange'));}, intersect(value) {intersect([{isIntersecting: value}]);}};
}

test('both controller rendering paths observe all wait cues and ease facial transitions', async t => {
  for (const fallback of [false, true]) {
    const h = await fixture(t, fallback), seen = new Set();
    h.guide.setState('listening'); h.advance(500);
    const before = h.guide.snapshot().facePose;
    h.guide.setState('thinking');
    assert.equal(h.guide.snapshot().facePose.faceBrowTilt, before.faceBrowTilt, 'state change does not snap brow geometry');
    h.guide.setExpression({kind: 'inquiry', intensity: .7});
    h.advance(19000, value => {if (value.thinkingCue?.kind) seen.add(value.thinkingCue.kind);});
    assert.deepEqual([...seen].sort(), ['check', 'consider', 'patient', 'soften']);
    assert.equal(h.guide.snapshot().thinkingCue.requestBound, true);
    assert.ok(h.guide.snapshot().facePose.faceBrowTilt <= .181);
    assert.ok(Math.abs(h.guide.snapshot().facePose.mouthSkew) <= .0251);
    if (fallback) assert.equal(h.guide.element.dataset.fallbackFormat, 'animated-svg-2d');
  }
});

test('hide, pause, offscreen, dismissal and reduced motion retire old waiting choreography without replay', async t => {
  const h = await fixture(t, true);
  for (const [stop, resume] of [
    [() => h.guide.setPaused(true), () => h.guide.setPaused(false)],
    [() => h.hide(true), () => h.hide(false)],
    [() => h.intersect(false), () => h.intersect(true)],
    [() => h.guide.setVisible(false), () => h.guide.setVisible(true)],
    [() => h.guide.setReducedMotion(true), () => h.guide.setReducedMotion(false)]
  ]) {
    h.guide.setState('idle'); h.guide.setState('thinking'); h.advance(2000);
    assert.equal(h.guide.snapshot().thinkingCue.active, true);
    stop(); assert.equal(h.guide.snapshot().thinkingCue.requestBound, false);
    assert.equal(h.guide.snapshot().thinkingCue.active, false);
    resume(); h.advance(8000);
    assert.equal(h.guide.snapshot().thinkingCue.requestBound, false, 'a restored view cannot replay the previous request');
    assert.equal(h.guide.snapshot().thinkingCue.active, false);
  }
});

test('interruption and errors clear waiting cues and previous celebration before the next response', async t => {
  const h = await fixture(t);
  h.guide.setState('thinking'); h.advance(2000);
  assert.equal(h.guide.snapshot().thinkingCue.kind, 'consider');
  h.guide.perform({mood: 'celebrate', gesture: 'confirm', intensity: .8, durationMs: 1000});
  h.guide.setState('listening');
  assert.equal(h.guide.snapshot().performance, null);
  assert.equal(h.guide.snapshot().thinkingCue, null);
  h.advance(600); assert.equal(h.guide.element.dataset.faceExpression, 'attentive');
  h.guide.perform({mood: 'celebrate', gesture: 'confirm', intensity: .8, durationMs: 1000});
  h.guide.setState('error');
  assert.equal(h.guide.snapshot().performance, null);
  h.advance(600); assert.equal(h.guide.element.dataset.faceExpression, 'reassuring');
  assert.equal(h.guide.snapshot().speechVisual.rippleActive, false);
  h.guide.setState('thinking'); h.advance(2000);
  assert.equal(h.guide.snapshot().thinkingCue.kind, 'consider', 'a fresh state starts its own request-local cues');
});
