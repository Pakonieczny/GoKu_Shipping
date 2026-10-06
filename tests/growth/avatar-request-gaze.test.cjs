'use strict';
// CPU + deterministic synthetic DOM coverage only. Not GPU/audio-device proof.
const test = require('node:test');
const assert = require('node:assert/strict');
const {JSDOM} = require('jsdom');
const avatar = require('../../brites-concierge-avatar.js');

const box = {left: 100, top: 100, width: 320, height: 280};
async function fixture(t, {fallback = false, reduced = false, anchor = null} = {}) {
  const dom = new JSDOM('<main><button id="shop">Shop</button><div id="mount"></div></main>', {url: 'https://sandbox.example/', pretendToBeVisual: true});
  const win = dom.window, doc = win.document, frames = new Map(), timers = new Map();
  let clock = 1000, hidden = false, frameId = 0, timerId = 0, observe, onFrame;
  Object.defineProperty(win.performance, 'now', {value: () => clock});
  Object.defineProperty(doc, 'hidden', {get: () => hidden});
  win.matchMedia = () => ({matches: reduced, addEventListener() {}, removeEventListener() {}});
  win.requestAnimationFrame = callback => {frames.set(++frameId, callback); return frameId;};
  win.cancelAnimationFrame = id => frames.delete(id);
  win.setTimeout = callback => {timers.set(++timerId, callback); return timerId;};
  win.clearTimeout = id => timers.delete(id);
  win.IntersectionObserver = class {constructor(callback) {observe = callback;} observe() {} disconnect() {observe = null;}};
  const engine = {setMotion() {}, render() {}, invalidate() {}, destroy() {}, snapshot() {return {};}, gazeAnchor() {return anchor;}};
  const guide = avatar.create({container: doc.getElementById('mount'), visible: true, greetingOnOpen: false, loadScene: async () => {
    if (fallback) throw Error('Synthetic WebGL unavailable');
    return {createAvatarScene(config) {onFrame = config.onFrame; return engine;}};
  }});
  guide.element.getBoundingClientRect = () => ({...box});
  guide.element.querySelector('svg').getBoundingClientRect = () => ({...box});
  await guide.ready;
  function pointer(x, y, {target = doc.getElementById('shop'), type = 'mouse'} = {}) {
    const event = new win.MouseEvent('pointermove', {bubbles: true, clientX: x, clientY: y});
    Object.defineProperty(event, 'pointerType', {value: type}); target.dispatchEvent(event);
  }
  function advance(milliseconds) {
    const until = clock + milliseconds;
    while (clock < until) {clock = Math.min(until, clock + 16); const jobs = Array.from(frames.values()); frames.clear(); jobs.forEach(fn => fn(clock));}
  }
  t.after(() => {guide.destroy(); win.close();});
  return {guide, win, doc, frames, pointer, advance, pose: () => onFrame?.(clock / 1000),
    hide(value) {hidden = value; doc.dispatchEvent(new win.Event('visibilitychange'));},
    intersect(value) {observe?.([{isIntersecting: value}]);}};
}

test('page-pointer mapping is centered on the face with positive y UP', () => {
  const anchor = {x: .5, y: 118 / 280};
  assert.deepEqual(avatar.pointerGaze(260, 218, box, anchor), {x: 0, y: 0});
  assert.ok(avatar.pointerGaze(260, 100, box, anchor).y > 0);
  assert.ok(avatar.pointerGaze(260, 350, box, anchor).y < 0);
  assert.equal(avatar.pointerGaze(10000, -10000, box, anchor).x, 1);
  assert.equal(avatar.pointerGaze(10000, -10000, box, anchor).y, 1);
  assert.equal(avatar.pointerGaze(NaN, 0, box, anchor), null);
  assert.equal(avatar.pointerGaze(0, Infinity, box, anchor), null);
  assert.equal(avatar.pointerGaze(0, 0, {...box, width: 0}, anchor), null);
});

test('rig head and eye agree on upward/downward gaze in every conversation state', () => {
  for (const state of avatar.STATES) {
    const up = avatar.poseFor({state, gaze: {x: 0, y: 1}}), down = avatar.poseFor({state, gaze: {x: 0, y: -1}});
    assert.ok(up.headPitch < 0, state + ': negative Three X rotation looks up');
    assert.ok(up.gazeY > 0, state + ': eye goes up');
    assert.ok(down.headPitch > 0, state + ': positive Three X rotation looks down');
    assert.ok(down.gazeY < 0, state + ': eye goes down');
    assert.ok(Math.abs(up.headPitch) <= .09 && Math.abs(up.gazeY) <= .08);
  }
});

test('stable idle does not perpetually float, sway, look around or rotate the ring', () => {
  const first = avatar.poseFor({state: 'idle', time: 1});
  for (const time of [4, 7, 15, 39, 120]) {
    const next = avatar.poseFor({state: 'idle', time});
    for (const key of ['bob', 'bodyYaw', 'bodyDepth', 'bodyRoll', 'headPitch', 'headYaw', 'headRoll', 'gazeX', 'gazeY', 'helloWave', 'armLiftRight', 'armLiftLeft', 'ringRotation']) assert.equal(next[key], first[key], key);
  }
  assert.equal(first.bob, 0); assert.equal(first.stanceScale, 1);
});

test('input/listening energy never produces speech, and output rests never invent a phrase loop', () => {
  const listening = avatar.poseFor({state: 'listening', level: 1, time: 24});
  assert.equal(listening.speechEnergy, 0); assert.equal(listening.mouthOpen, 0); assert.equal(listening.phraseGesture, 0);
  assert.equal(avatar.poseFor({state: 'speaking', level: 0, time: 4}).mouthOpen, 0);
  assert.equal(avatar.poseFor({state: 'speaking', level: 1, time: 4}).phraseGesture, 0);
  assert.ok(avatar.poseFor({state: 'speaking', level: 1, speechBeatElapsed: .26}).phraseGesture > 0);
  assert.equal(avatar.poseFor({state: 'speaking', level: 1, speechBeatElapsed: 4}).phraseGesture, 0);
});

test('graphic face signatures are distinct geometry controls, bounded and reduced-motion stable', () => {
  const signatures = new Set();
  for (const name of Object.keys(avatar.FACE_EXPRESSIONS)) {
    const face = avatar.FACE_EXPRESSIONS[name]; signatures.add(JSON.stringify(face));
    assert.ok(face.faceBrowLift >= -1 && face.faceBrowLift <= 1);
    assert.ok(face.faceBrowTilt >= -1 && face.faceBrowTilt <= 1);
    for (const key of ['eyeSmile', 'cheekGlow', 'smileCurve', 'faceSignal']) assert.ok(face[key] >= 0 && face[key] <= 1, key);
  }
  assert.equal(signatures.size, 7);
  for (const state of avatar.STATES) {
    const first = avatar.poseFor({state, reducedMotion: true, time: 1});
    assert.deepEqual(avatar.poseFor({state, reducedMotion: true, time: 50}), first);
  }
});

test('cursor on ordinary shop content outside avatar remains tracked; frame exit does not reset it', async t => {
  const f = await fixture(t);
  f.pointer(650, 100); const target = f.guide.snapshot().gaze.target;
  assert.ok(target.x > 0 && target.y > 0, 'document-level event, outside avatar');
  f.guide.element.dispatchEvent(new f.win.MouseEvent('pointerleave', {bubbles: false}));
  assert.deepEqual(f.guide.snapshot().gaze.target, target);
  f.advance(700); assert.ok(f.pose().gazeX > .06); assert.ok(f.pose().headPitch < 0);
  f.advance(1000);
  assert.equal(f.frames.size, 0, 'damping stops when settled');
});

test('eyes lead the slower head without snapping or overshooting after cursor reversal', async t => {
  const f = await fixture(t); f.pointer(650, 100); f.advance(64);
  const early = f.guide.snapshot().gaze;
  assert.ok(early.eye.x > early.head.x && early.head.x > 0);
  assert.ok(early.eye.x < early.target.x, 'first motion is damped');
  f.pointer(0, 350); f.advance(700); const final = f.guide.snapshot().gaze;
  assert.ok(final.eye.x < 0 && final.head.x < 0);
  assert.ok(final.eye.y < 0 && final.head.y < 0);
  for (const channel of [final.eye, final.head]) for (const axis of ['x', 'y']) assert.ok(Math.abs(channel[axis]) <= 1);
});

test('WebGL gaze uses scene-projected face center instead of the whole-frame midpoint', async t => {
  const f = await fixture(t, {anchor: {x: .5, y: .25}});
  f.pointer(260, 170); assert.deepEqual(f.guide.snapshot().gaze.target, {x: 0, y: 0});
  f.pointer(260, 100); assert.ok(f.guide.snapshot().gaze.target.y > 0);
});

test('fallback face paths and visible graphic details change with real emotion/state', async t => {
  const f = await fixture(t, {fallback: true});
  const brow = f.guide.element.querySelector('.brites-avatar__brow--left'), smile = f.guide.element.querySelector('.brites-avatar__smile-signal');
  const neutral = {brow: brow.getAttribute('d'), smile: smile.getAttribute('d')};
  f.guide.setState('listening'); assert.equal(f.guide.element.dataset.faceExpression, 'attentive'); assert.notEqual(brow.getAttribute('d'), neutral.brow);
  f.guide.setEmotion('warm'); assert.equal(f.guide.element.dataset.faceExpression, 'warm'); assert.notEqual(smile.getAttribute('d'), neutral.smile);
  assert.equal(f.guide.element.querySelectorAll('.brites-avatar__eye path').length, 2);
  const left=f.guide.element.querySelector('.brites-avatar__ribbon--left'),ribbon=left.getAttribute('d');f.guide.setEmotion('curious');assert.notEqual(left.getAttribute('d'),ribbon);
  assert.equal(f.guide.element.querySelector('.brites-avatar__halo'),null);
  f.pointer(650, 100); f.advance(700);
  assert.ok(parseFloat(f.guide.element.style.getPropertyValue('--brites-gaze-y')) < 0, 'SVG screen coordinate points up');
});

test('pause, hidden and offscreen cancel pointer motion and never replay stale gaze on resume', async t => {
  const f = await fixture(t);
  for (const [stop, start] of [[() => f.guide.setPaused(true), () => f.guide.setPaused(false)], [() => f.hide(true), () => f.hide(false)], [() => f.intersect(false), () => f.intersect(true)], [() => f.guide.setVisible(false), () => f.guide.setVisible(true)]]) {
    f.pointer(650, 100); assert.ok(f.frames.size > 0); stop(); assert.equal(f.frames.size, 0);
    f.pointer(0, 350); assert.deepEqual(f.guide.snapshot().gaze.target, {x: 0, y: 0});
    start(); f.advance(100); assert.deepEqual(f.guide.snapshot().gaze.eye, {x: 0, y: 0});
  }
});

test('reduced motion follows pointer with a static immediate pose, no timed smoothing', async t => {
  const f = await fixture(t, {reduced: true}); f.pointer(650, 100);
  assert.deepEqual(f.guide.snapshot().gaze.eye, f.guide.snapshot().gaze.target);
  assert.deepEqual(f.guide.snapshot().gaze.head, f.guide.snapshot().gaze.target);
  assert.equal(f.frames.size, 0); const pose = f.pose(); f.advance(1000); assert.deepEqual(f.pose(), pose);
});

test('touch scroll is ignored, page exit/blur recenters, destroy removes listeners and RAF', async t => {
  const f = await fixture(t); f.pointer(650, 100, {type: 'touch'}); assert.deepEqual(f.guide.snapshot().gaze.target, {x: 0, y: 0});
  f.pointer(650, 100); f.doc.dispatchEvent(new f.win.MouseEvent('pointerout', {relatedTarget: f.doc.getElementById('shop')})); assert.ok(f.guide.snapshot().gaze.target.x > 0);
  f.doc.dispatchEvent(new f.win.MouseEvent('pointerout', {relatedTarget: null})); assert.deepEqual(f.guide.snapshot().gaze.target, {x: 0, y: 0});
  f.pointer(650, 100); f.win.dispatchEvent(new f.win.Event('blur')); assert.deepEqual(f.guide.snapshot().gaze.target, {x: 0, y: 0});
  f.pointer(650, 100); f.guide.destroy(); assert.equal(f.frames.size, 0); const state = f.guide.snapshot().gaze;
  f.pointer(0, 350); f.advance(100); assert.deepEqual(f.guide.snapshot().gaze, state);
});

test('explicit product attention takes precedence, clears cleanly and leaves page gaze recoverable', async t => {
  const f = await fixture(t); f.pointer(650, 100); f.advance(700);
  f.guide.focusProduct({x: -.8, y: -.2}); f.advance(700); assert.ok(f.pose().gazeX < 0);
  f.guide.clearFocus(); assert.ok(f.pose().gazeX > 0); assert.ok(f.pose().gazeY > 0);
});
