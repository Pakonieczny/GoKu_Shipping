'use strict';
// Controller/DOM behavior only: CSS animation appearance, GPU and FPS still
// require browser checks. No mock scene is evidence of actual 3-D rendering.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const {JSDOM} = require('jsdom');
const avatar = require('../../brites-concierge-avatar.js');
const tick = () => new Promise(resolve => setImmediate(resolve));
function harness(t, {reducedMotion = false, loader} = {}) {
  const dom = new JSDOM('<div id="avatar"></div>', {url: 'https://sandbox.example/', pretendToBeVisual: true});
  const win = dom.window; let hidden = false, intersect;
  Object.defineProperty(win.document, 'hidden', {get: () => hidden});
  const listeners = new Set();
  win.matchMedia = () => ({matches: reducedMotion, addEventListener: (name, cb) => listeners.add(cb), removeEventListener: (name, cb) => listeners.delete(cb)});
  win.IntersectionObserver = class {constructor(cb) {intersect = cb;} observe() {} disconnect() {}};
  let loads = 0;
  const api = avatar.create({container: win.document.getElementById('avatar'), visible: true,
    loadScene: async () => {loads++; return loader ? loader() : Promise.reject(Error('No WebGL context'));}});
  t.after(() => {api.destroy(); win.close();});
  return {api, win, loads: () => loads,
    hide: value => {hidden = value; win.document.dispatchEvent(new win.Event('visibilitychange'));},
    intersect: value => intersect([{isIntersecting: value}]),
    motion: value => {for (const cb of listeners) cb({matches: value});}};
}
test('original digital eye uses bounded deformations, state colors and non-color expression cues', () => {
  const colors = new Set(), emotions = new Set();
  for (const state of avatar.STATES) {
    const pose = avatar.poseFor({state, time: 1.2, elapsed: .6, level: .5});
    assert.match(pose.eyeColor, /^#[a-f\d]{6}$/i); colors.add(pose.eyeColor); emotions.add(pose.emotion);
    for (const [key, value] of Object.entries(pose)) if (typeof value === 'number') assert.ok(Number.isFinite(value), key);
    assert.ok(pose.eyeScaleX > .8 && pose.eyeScaleX < 1.2); assert.ok(pose.eyeScaleY > .6 && pose.eyeScaleY < 1.2);
    assert.ok(Math.abs(pose.eyeDeformation) <= .3);
  }
  assert.equal(colors.size, 6); assert.equal(emotions.size, 6);
  assert.equal(avatar.poseFor().eyeColor, '#4aa8ff');
  assert.ok(avatar.poseFor({time: 3.39}).eyeOpen < avatar.poseFor({time: 1}).eyeOpen);
});
test('unbounded time, elapsed, gaze and amplitude cannot create non-finite rig data', () => {
  for (const state of avatar.STATES) for (const bad of [Infinity, -Infinity, NaN]) {
    const pose = avatar.poseFor({state, time: bad, elapsed: bad, level: bad, gaze: {x: bad, y: bad}});
    for (const [key, value] of Object.entries(pose)) if (typeof value === 'number') assert.ok(Number.isFinite(value), key);
  }
});
test('calm and reassuring contexts remove celebratory bounce and large gestures', () => {
  for (const emotion of ['calm', 'reassuring']) {
    const success = avatar.poseFor({state: 'success', time: 0, elapsed: .6, emotion});
    assert.equal(success.emotion, emotion); assert.equal(success.bob, 0); assert.equal(success.headRoll, -.012); assert.equal(success.armLift, .025);
  }
  assert.ok(avatar.poseFor({state: 'success', time: 0, elapsed: .6, emotion: 'celebrate'}).bob > .1);
  assert.equal(avatar.validEmotion('untrusted'), null);
});
test('reduced motion preserves every mood without timed blinks, breathing or ring rotation', () => {
  for (const state of avatar.STATES) for (const emotion of avatar.EMOTIONS) {
    assert.deepEqual(avatar.poseFor({state, emotion, time: 1, elapsed: .6, reducedMotion: true}), avatar.poseFor({state, emotion, time: 60, elapsed: 60, reducedMotion: true}));
  }
});
test('WebGL failure gives an explicitly animated 2-D vector robot, with no portrait fetch', async t => {
  const h = harness(t), result = await h.api.ready;
  assert.equal(result.mode, 'fallback'); assert.equal(result.fallback.format, 'animated_svg_2d'); assert.equal(result.fallback.animated, true);
  assert.equal(result.fallback.reason, 'WebGL rendering is unavailable'); assert.equal(h.api.element.querySelector('img'), null);
  assert.equal(h.api.element.dataset.fallbackFormat, 'animated-svg-2d');
  assert.equal(h.api.element.querySelectorAll('.brites-avatar__eye').length, 1);
  assert.match(h.api.element.getAttribute('aria-label'), /2-D companion; 3-D unavailable/);
  assert.match(h.api.element.querySelector('.brites-avatar__caption').textContent, /Ready to help.*2-D/);
  h.api.setState('thinking'); assert.match(h.api.element.querySelector('.brites-avatar__caption').textContent, /Thinking/);
  assert.equal(h.api.element.style.getPropertyValue('--brites-eye-color'), '#ab87ff');
  h.api.setState('speaking'); h.api.setLevel(.8);
  assert.equal(h.api.element.style.getPropertyValue('--brites-speech-level'), '0.8');
  assert.equal(h.api.element.style.getPropertyValue('--brites-speech-scale'), '1.112');
  h.api.setEmotion('calm'); h.api.setState('success'); assert.match(h.api.element.querySelector('.brites-avatar__caption').textContent, /Here with you/);
  assert.equal(h.api.snapshot().emotion, 'calm');
});
test('fallback stops under user pause, dismissal, off-screen, hidden document and reduced motion', async t => {
  const h = harness(t); await h.api.ready;
  for (const [stop, resume] of [
    [() => h.api.setPaused(true), () => h.api.setPaused(false)],
    [() => h.api.setVisible(false), () => h.api.setVisible(true)],
    [() => h.intersect(false), () => h.intersect(true)],
    [() => h.hide(true), () => h.hide(false)],
    [() => h.motion(true), () => h.motion(false)]
  ]) {
    stop(); assert.equal(h.api.snapshot().animated, false); assert.equal(h.api.snapshot().fallback.animated, false);
    assert.notEqual(h.api.element.dataset.motion, 'running'); resume(); assert.equal(h.api.snapshot().fallback.animated, true);
  }
  assert.equal(h.loads(), 1, 'ordinary state/visibility changes must not retry a failed scene');
  h.api.destroy(); assert.equal(h.api.snapshot().animated, false);
});
test('user pause stops the WebGL scene and resumed rendering receives the explicit emotion', async t => {
  const motions = [], poses = [];
  const engine = {setMotion: value => motions.push(value), render: pose => poses.push(pose), destroy() {}, invalidate() {}, snapshot: () => ({animated: true})};
  const h = harness(t, {loader: () => ({createAvatarScene: () => engine})}); await h.api.ready; await tick();
  h.api.setPaused(true); assert.equal(motions.at(-1).active, false); assert.equal(h.api.snapshot().animated, false);
  h.api.setEmotion('reassuring'); const before = poses.length; h.api.setState('success'); assert.equal(poses.length, before);
  h.api.setPaused(false); assert.equal(motions.at(-1).active, true); assert.equal(poses.at(-1).emotion, 'reassuring');
});
test('vector animation stylesheet has explicit pause/reduced-motion bounds and no raster fallback', () => {
  const css = fs.readFileSync(require.resolve('../../brites-concierge-avatar.css'), 'utf8');
  assert.match(css, /@keyframes britesRobotBlink/); assert.match(css, /@keyframes britesRobotSpeak/);
  assert.match(css, /data-motion=paused.*animation-play-state:paused/);
  assert.match(css, /prefers-reduced-motion:reduce.*animation:none/);
  assert.doesNotMatch(css, /data-image=ready/);
  assert.doesNotMatch(fs.readFileSync(require.resolve('../../brites-concierge-avatar.js'), 'utf8'), /avatar-concept\.png/);
});
