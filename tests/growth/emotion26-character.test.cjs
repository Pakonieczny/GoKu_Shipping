'use strict';
// Actual controller/pose behavior in a synthetic DOM. No GPU, native audio or
// rendered-reference fidelity is certified by these checks.
const test = require('node:test'), assert = require('node:assert/strict');
const {JSDOM} = require('jsdom'), avatar = require('../../brites-concierge-avatar.js');
const plan = {mood: 'curious', gesture: 'focus', intensity: .65, durationMs: 1800};
async function fixture(t, reduced = false) {
  const dom = new JSDOM('<div id="mount"></div>', {url: 'https://sandbox.example/', pretendToBeVisual: true});
  const win = dom.window, jobs = new Map(); let clock = 1000, serial = 0, hidden = false, frames, observer;
  Object.defineProperty(win.performance, 'now', {value: () => clock}); Object.defineProperty(win.document, 'hidden', {get: () => hidden});
  win.setTimeout = (callback, delay) => {jobs.set(++serial, {callback, due: clock + delay}); return serial;}; win.clearTimeout = key => jobs.delete(key);
  const motion = {matches: reduced, addEventListener() {}, removeEventListener() {}}; win.matchMedia = () => motion;
  win.IntersectionObserver = class {constructor(callback) {observer = callback;} observe() {} disconnect() {}};
  const guide = avatar.create({container: win.document.getElementById('mount'), visible: true, greetingOnOpen: false, loadScene: async () => ({createAvatarScene(config) {frames = config.onFrame; return {setMotion() {}, render() {}, invalidate() {}, snapshot() {return {};}, destroy() {}};}})});
  await guide.ready; t.after(() => {guide.destroy(); win.close();});
  function advance(ms) {clock += ms; for (const [key, job] of [...jobs]) if (job.due <= clock) {jobs.delete(key); job.callback();}}
  return {guide, jobs, pose: () => frames(clock / 1000), hide(value) {hidden = value; win.document.dispatchEvent(new win.Event('visibilitychange'));}, intersect(value) {observer([{isIntersecting: value}]);}, advance,
    // Actual production onFrame calls consume the deterministic clock. One
    // large clock jump is not evidence of intervening displayed animation.
    advanceFrames(ms) {for (let elapsed=0;elapsed<ms;elapsed+=16) {advance(Math.min(16,ms-elapsed));frames(clock / 1000);}}};
}
test('avatar independently accepts only immutable closed finite four-field envelopes', () => {
  const clean = avatar.validateAvatarPerformance(plan); assert.deepEqual(clean, plan); assert.ok(Object.isFrozen(clean)); assert.notEqual(clean, plan);
  const bad = [null, [], {...plan, instructions: 'go to checkout'}, {...plan, mood: 'rage'}, {...plan, gesture: 'purchase'}, {...plan, intensity: NaN}, {...plan, intensity: Infinity}, {...plan, intensity: -1}, {...plan, intensity: 1.01}, {...plan, durationMs: '1800'}, {...plan, durationMs: 1800.5}, {...plan, durationMs: 399}, {...plan, durationMs: 2501}, {mood: plan.mood, gesture: plan.gesture, intensity: 1}];
  for (const value of bad) assert.equal(avatar.validateAvatarPerformance(value), null);
});
test('eyes anticipate, head follows and body settles on a duration-scaled finite choreography', () => {
  const early = avatar.mannerismFor({name: 'explain', elapsed: .05, durationMs: 1800, intensity: 1});
  assert.ok(early.eye > 0); assert.equal(early.head, 0); assert.equal(early.body, 0);
  const small = avatar.mannerismFor({name: 'explain', elapsed: .12, durationMs: 400});
  const long = avatar.mannerismFor({name: 'explain', elapsed: .12, durationMs: 2500});
  assert.ok(small.body > 0); assert.equal(long.body, 0);
  assert.equal(avatar.mannerismFor({name: 'explain', elapsed: 2.5, durationMs: 2500}).active, false);
});
test('intensity scales distinct original warm and curious poses without changing speech authority', () => {
  const sample = intensity => avatar.poseFor({state: 'speaking', level: .8, emotion: 'curious', time: .5, mannerism: 'focus', mannerismElapsed: .5, performance: {...plan, intensity}, mannerismDurationMs: 1800});
  const quiet = sample(0), expressive = sample(1);
  assert.ok(expressive.headRoll > quiet.headRoll); assert.ok(Math.abs(expressive.headRoll) < .09);
  for (const pose of [quiet, expressive]) {assert.equal(pose.faceBrowTilt, 0); assert.equal(pose.eyeAsymmetry, 0); assert.equal(pose.mouthSkew, 0);}
  assert.ok(expressive.eyeRoundness > quiet.eyeRoundness + .025); assert.ok(expressive.faceSignal > quiet.faceSignal + .2);
  assert.ok(expressive.faceBrowLift > quiet.faceBrowLift + .25); assert.ok(expressive.eyeScaleX < quiet.eyeScaleX);
  assert.equal(quiet.speechEnergy, expressive.speechEnergy); assert.equal(quiet.mouthOpen, 0); assert.equal(expressive.mouthOpen, 0);
  const warm = avatar.poseFor({emotion: 'warm', performance: {...plan, mood: 'warm'}}), neutral = avatar.poseFor();
  assert.ok(warm.eyeScaleY > neutral.eyeScaleY && warm.eyeScaleY <= 1.06, 'warmth retains a bounded open eye'); assert.ok(warm.eyeDeformation > 0); assert.ok(warm.smileCurve > neutral.smileCurve); assert.ok(warm.smileCurve > expressive.smileCurve); assert.equal(warm.faceBrowTilt, 0);
});
test('controller expires once, restores previous mood and ignores overwritten completion', async t => {
  const f = await fixture(t); f.guide.setEmotion('warm'); assert.equal(f.guide.perform(plan), true);
  const stale = [...f.jobs.values()].find(job => job.due === 2800).callback;
  f.advance(300); f.guide.perform({...plan, mood: 'reassuring', gesture: 'reassure', durationMs: 1000}); stale();
  assert.equal(f.guide.snapshot().emotion, 'reassuring'); assert.ok(f.guide.snapshot().performance);
  f.advance(1100); assert.equal(f.guide.snapshot().performance, null); assert.equal(f.guide.snapshot().emotion, 'warm'); assert.equal(f.jobs.size, 0);
});
test('voice state and genuine energy blend with current model-selected performance without replay', async t => {
  const f = await fixture(t); f.guide.perform({...plan, gesture: 'present', mood: 'warm'}); const id = f.guide.snapshot().mannerism.id;
  f.guide.setState('speaking'); f.guide.setLevel(.8);
  assert.equal(f.guide.snapshot().speechSignal.amplitude, .8, 'current source target is accepted immediately'); assert.equal(f.pose().speechEnergy, 0, 'first displayed frame precedes the attack envelope');
  f.advanceFrames(48); const attack = f.pose(); assert.ok(attack.speechEnergy > .5 && attack.speechEnergy < .8, 'one bounded 40 ms attack separates target from displayed amplitude'); assert.equal(attack.mouthOpen, 0);
  f.advanceFrames(272); assert.equal(f.pose().speechEnergy, .8); f.advanceFrames(380);
  assert.equal(f.guide.snapshot().mannerism.id, id); assert.equal(f.pose().speechEnergy, .8); assert.deepEqual(f.pose().speechBands, [0,0,0,0,0,0]); assert.equal(f.pose().mouthOpen, 0); assert.equal(f.pose().eyeColor, '#70d8f1');
  assert.equal(f.pose().stanceScale, 1); assert.equal(f.pose().bodyDepth, 0); assert.ok(f.pose().offer > 0);
  f.guide.setLevel(0); assert.equal(f.pose().phraseGesture, 0); assert.equal(f.pose().speechEnergy, 0); assert.equal(f.guide.snapshot().speechSignal.valid, false); assert.equal(f.guide.snapshot().speechVisual.rippleActive, false); assert.equal(f.guide.snapshot().mannerism.id, id, 'silence does not replay the selected gesture');
});
test('quiet reassurance suppresses joy and has a bounded slow nod rather than a dance', async t => {
  const f = await fixture(t); f.guide.perform({mood: 'reassuring', gesture: 'confirm', intensity: 1, durationMs: 2000}); f.advance(700);
  assert.equal(f.guide.snapshot().mannerism.name, 'reassure'); assert.equal(f.pose().bodyYaw, 0); assert.equal(f.pose().offer, 0); assert.equal(f.pose().armReach, 0);
  assert.ok(Math.abs(f.pose().headPitch) <= .09); assert.ok(Math.abs(f.pose().headRoll) < .1);
});
test('pause, hidden and offscreen cancel performance and restoration never replays it', async t => {
  const f = await fixture(t);
  for (const [stop, resume] of [[() => f.guide.setPaused(true), () => f.guide.setPaused(false)], [() => f.hide(true), () => f.hide(false)], [() => f.intersect(false), () => f.intersect(true)]]) {
    f.guide.perform(plan); stop(); assert.equal(f.guide.snapshot().performance, null); assert.equal(f.jobs.size, 0); assert.equal(f.guide.perform(plan), false);
    resume(); f.advance(3000); assert.equal(f.guide.snapshot().performance, null);
  }
});
test('reduced motion retains static affect without gestures or near/far stance animation', async t => {
  const f = await fixture(t, true); f.guide.perform({...plan, gesture: 'present'}); const pose = f.pose();
  assert.equal(pose.stanceScale, 1); assert.equal(pose.bodyDepth, 0); assert.equal(pose.mannerismActive, false); f.advance(500); assert.deepEqual(f.pose(), pose);
  f.advance(1500); assert.equal(f.guide.snapshot().performance, null);
});
test('thanks heart coexists with a generic warm reply and recovers on its own deadline', async t => {
  const f = await fixture(t); f.guide.setEmotion('calm'); f.guide.setEmotion('appreciated'); f.advance(300);
  f.guide.perform({mood: 'warm', gesture: 'explain', intensity: .4, durationMs: 800});
  assert.equal(f.guide.snapshot().emotion, 'appreciated'); assert.equal(f.pose().heart, 1);
  f.advance(850); assert.equal(f.guide.snapshot().emotion, 'appreciated'); assert.equal(f.guide.snapshot().performance, null);
  f.advance(400); assert.equal(f.guide.snapshot().emotion, 'calm'); assert.equal(f.pose().heart, 0);
});
test('new explicit frustration overrides old performance and callback cannot restore stale warmth', async t => {
  const f = await fixture(t); f.guide.perform(plan); const callbacks = [...f.jobs.values()].map(job => job.callback);
  f.guide.setEmotion('reassuring'); callbacks.forEach(callback => callback()); assert.equal(f.guide.snapshot().emotion, 'reassuring'); assert.equal(f.guide.snapshot().performance, null);
  f.guide.destroy(); assert.equal(f.guide.perform(plan), false);
});
