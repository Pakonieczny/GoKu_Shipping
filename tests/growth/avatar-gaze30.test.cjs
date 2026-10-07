'use strict';
// Authored deterministic DOM/CPU motion evidence. No GPU, perceived comfort,
// FPS, speech audibility or physical microphone claim is made here.
const test = require('node:test'), assert = require('node:assert/strict');
const {JSDOM} = require('jsdom');
const {readFileSync} = require('node:fs');
const avatar = require('../../brites-concierge-avatar.js');

async function fixture(t, {fallback = false, reduced = false, resizeObserver = true} = {}) {
  const dom = new JSDOM('<main><a id="one" href="/products/one">One</a><a id="two" href="/products/two">Two</a><div id="mount"></div></main>', {url: 'https://sandbox.example/', pretendToBeVisual: true});
  const win = dom.window, doc = win.document, frames = new Map(), timers = new Map();
  let milliseconds = 1000, hidden = false, frameId = 0, timerId = 0, onFrame, observed, geometryCallback, geometryTarget, geometryDisconnected = false;
  let box = {left: 100, top: 100, width: 320, height: 280};
  Object.defineProperty(win.performance, 'now', {value: () => milliseconds});
  Object.defineProperty(doc, 'hidden', {get: () => hidden});
  win.matchMedia = () => ({matches: reduced, addEventListener() {}, removeEventListener() {}});
  win.requestAnimationFrame = callback => {frames.set(++frameId, callback); return frameId;};
  win.cancelAnimationFrame = id => frames.delete(id);
  win.setTimeout = (callback, delay) => {timers.set(++timerId, {callback, at: milliseconds + delay}); return timerId;};
  win.clearTimeout = id => timers.delete(id);
  win.IntersectionObserver = class {constructor(callback) {observed = callback;} observe() {} disconnect() {observed = null;}};
  if (resizeObserver) win.ResizeObserver = class {constructor(callback) {geometryCallback = callback;} observe(target) {geometryTarget = target;} disconnect() {geometryDisconnected = true;}};
  const engine = {setMotion() {}, render() {}, invalidate() {}, destroy() {}, snapshot() {return {};}, gazeAnchor() {return {x: .5, y: 118 / 280};}};
  const guide = avatar.create({container: doc.getElementById('mount'), visible: true, greetingOnOpen: false, loadScene: async () => {
    if (fallback) throw Error('Authored no-WebGL test');
    return {createAvatarScene(config) {onFrame = config.onFrame; return engine;}};
  }});
  guide.element.getBoundingClientRect = () => ({...box});
  guide.element.querySelector('svg').getBoundingClientRect = () => ({...box});
  await guide.ready;
  const pose = () => onFrame ? onFrame(milliseconds / 1000) : guide.snapshot().gaze.pose;
  function frame(ms = 16) {
    milliseconds += ms;
    for (const [id, timer] of [...timers]) if (timer.at <= milliseconds) {timers.delete(id); timer.callback();}
    const jobs = [...frames.values()]; frames.clear(); jobs.forEach(callback => callback(milliseconds));
    if (onFrame) onFrame(milliseconds / 1000);
    return guide.snapshot();
  }
  function advance(ms, step = 16) {const until = milliseconds + ms; while (milliseconds < until) frame(Math.min(step, until - milliseconds));}
  function pointer(x, y, listing = 'one') {
    const event = new win.MouseEvent('pointermove', {bubbles: true, clientX: x, clientY: y});
    Object.defineProperty(event, 'pointerType', {value: 'mouse'}); doc.getElementById(listing).dispatchEvent(event);
  }
  t.after(() => {guide.destroy(); win.close();});
  return {guide, win, doc, frame, advance, pointer, pose, frames, move(next) {box = {...box, ...next};}, elapse(ms) {milliseconds += ms;}, stageResize(next = {}) {box = {...box, ...next}; geometryCallback?.([{target: geometryTarget}]);}, windowResize(next = {}) {box = {...box, ...next}; win.dispatchEvent(new win.Event('resize'));}, geometry() {return {stageObserved: geometryTarget === doc.getElementById('mount'), disconnected: geometryDisconnected};}, jump(ms) {milliseconds += ms; return pose();}, at(ms) {milliseconds = ms; return pose();}, hide(value) {hidden = value; doc.dispatchEvent(new win.Event('visibilitychange'));}, intersect(value) {observed?.([{isIntersecting: value}]);}};
}
function samePose(before, after, label) {
  for (const key of ['eye', 'head', 'velocity', 'pose']) assert.deepEqual(after.gaze[key], before.gaze[key], label + ': ' + key + ' does not reset');
}

for (const profileName of ['eye', 'head', 'headPose']) {
  test(profileName + ' motion retains bounded velocity and acceleration through 20,000 target reversals', () => {
    const profile = avatar.GAZE_MOTION[profileName]; let channel = {position: 0, velocity: 0};
    for (let index = 0; index < 20000; index++) {
      const delta = [.004, .008, .016, .033][index % 4], target = profile.max * Math.sin(index * .04) * (index % 300 < 150 ? 1 : -1);
      const next = avatar.advanceGazeAxis(channel, target, delta, profile);
      assert.ok(next.position >= profile.min && next.position <= profile.max);
      assert.ok(Math.abs(next.velocity) <= profile.velocity + 1e-9);
      assert.ok(Math.abs(next.velocity - channel.velocity) <= profile.acceleration * delta + 1e-9);
      assert.ok(Math.abs(next.position - channel.position) <= profile.velocity * delta + 1e-9);
      channel = next;
    }
  });
  test(profileName + ' response agrees across 30, 60 and 120 Hz and ignores invalid time', () => {
    const profile = avatar.GAZE_MOTION[profileName], positions = [];
    for (const hz of [30, 60, 120]) {
      let channel = {position: 0, velocity: 0};
      for (let index = 0; index < hz; index++) channel = avatar.advanceGazeAxis(channel, profile.max * .8, 1 / hz, profile);
      positions.push(channel.position);
      for (const invalid of [NaN, Infinity, -1, 0]) assert.deepEqual(avatar.advanceGazeAxis(channel, 1, invalid, profile), channel);
    }
    assert.ok(Math.max(...positions) - Math.min(...positions) < .0002);
    assert.deepEqual(avatar.advanceGazeAxis({position: 0, velocity: 0}, NaN, .016, profile), {position: 0, velocity: 0});
  });
}

for (const fallback of [false, true]) test((fallback ? 'SVG' : 'scene-pose') + ' rapid adjacent-card enter/leave never resets face positions or restarts a hand gesture', async t => {
  const f = await fixture(t, {fallback});
  f.pointer(340, 180); f.advance(300);
  for (let index = 0; index < 80; index++) {
    const before = f.guide.snapshot();
    f.guide.clearFocus(); samePose(before, f.guide.snapshot(), 'card leave');
    f.guide.focusProduct({x: index % 2 ? -1 : 1, y: index % 3 ? -.6 : .8, source: 'hover'});
    samePose(before, f.guide.snapshot(), 'adjacent card enter');
    f.pointer(index % 2 ? 315 : 340, 180, index % 2 ? 'two' : 'one');
    samePose(before, f.guide.snapshot(), 'same-clock pointer move');
    const after = f.frame(index % 2 ? 8 : 16);
    for (const [kind, profile] of [['eye', avatar.GAZE_MOTION.eye], ['head', avatar.GAZE_MOTION.head]]) for (const axis of ['x', 'y']) {
      assert.ok(Math.abs(after.gaze.velocity[kind][axis]) <= profile.velocity);
      assert.ok(Math.abs(after.gaze[kind][axis] - before.gaze[kind][axis]) <= profile.velocity * .016 + 1e-8);
    }
    assert.equal(after.mannerism.id, 0, 'hover never produces repeated explanation gestures');
    assert.equal(after.gaze.source, 'pointer', 'moving cursor outranks hovered card center');
    if (fallback) assert.equal(f.guide.element.style.getPropertyValue('--brites-focus-head'), '', 'no second unsmoothed focus rotation');
  }
});

test('hover dwell changes destination only, then a new pointer resumes immediately without a center reset', async t => {
  const f = await fixture(t); f.pointer(260, 218); f.guide.focusProduct({x: -.9, y: .7, source: 'hover'});
  f.advance(120); assert.equal(f.guide.snapshot().gaze.source, 'pointer');
  const before = f.guide.snapshot(); f.frame(24); const dwell = f.guide.snapshot();
  assert.equal(dwell.gaze.source, 'hover'); assert.ok(dwell.gaze.eye.x < before.gaze.eye.x);
  assert.ok(Math.abs(dwell.gaze.eye.x - before.gaze.eye.x) < .03, 'dwell starts gently rather than jumping to center');
  f.advance(700); const held = f.guide.snapshot(); assert.ok(held.gaze.eye.x < -.85);
  f.pointer(340, 218); samePose(held, f.guide.snapshot(), 'returning cursor');
  assert.equal(f.guide.snapshot().gaze.source, 'pointer'); f.frame(); assert.ok(f.guide.snapshot().gaze.eye.x > held.gaze.eye.x);
});

test('requested product focus keeps authority while clearFocus smoothly returns to the newest cursor', async t => {
  const f = await fixture(t); f.pointer(340, 180); f.advance(600);
  const before = f.guide.snapshot(); f.guide.focusProduct({x: -.8, y: -.3, source: 'presentation'}); samePose(before, f.guide.snapshot(), 'requested focus');
  f.advance(700); const held = f.guide.snapshot(); assert.ok(held.gaze.eye.x < -.79);
  f.pointer(650, 100); samePose(held, f.guide.snapshot(), 'pointer while presenting'); assert.equal(f.guide.snapshot().gaze.source, 'presentation');
  f.guide.clearFocus(); samePose(held, f.guide.snapshot(), 'requested focus release'); f.advance(700);
  assert.ok(f.guide.snapshot().gaze.eye.x > .98); assert.ok(f.pose().headPitch < 0, 'upward pointer keeps the correct pitch sign');
});

test('moving the guide recomputes the saved pointer without a gaze snap or a synthetic pointer event', async t => {
  const f = await fixture(t); f.pointer(650, 100); f.advance(700); const before = f.guide.snapshot();
  f.move({left: 900, top: 500}); samePose(before, f.guide.snapshot(), 'guide geometry move');
  const after = f.frame(); assert.ok(after.gaze.target.x < 0); assert.ok(after.gaze.target.y > 0);
  assert.ok(Math.abs(after.gaze.eye.x - before.gaze.eye.x) <= avatar.GAZE_MOTION.eye.velocity * .016);
  assert.ok(Math.abs(after.gaze.pose.headYaw.position - before.gaze.pose.headYaw.position) <= .42 * .016);
  f.advance(800); assert.ok(f.guide.snapshot().gaze.eye.x < -.98);
});

test('duplicate hover focus refreshes moved stage geometry immediately without restarting the trajectory', async t => {
  const f = await fixture(t, {fallback: true}); f.pointer(650, 100);
  f.guide.focusProduct({x: .8, y: .4, source: 'hover'}); f.advance(90);
  const before = f.guide.snapshot(); assert.equal(before.gaze.source, 'pointer'); assert.ok(before.gaze.target.x > 0);
  f.move({left: 900, top: 500});
  f.guide.focusProduct({x: .8, y: .4, source: 'hover'});
  const refreshed = f.guide.snapshot(); samePose(before, refreshed, 'duplicate moved focus');
  assert.ok(refreshed.gaze.target.x < 0, 'saved pointer is remapped before the next render frame');
  assert.equal(refreshed.productFocus.at, before.productFocus.at, 'the attention episode retains its clock');
  assert.equal(refreshed.mannerism.id, before.mannerism.id);
  f.frame(); assert.ok(Number.isFinite(f.guide.snapshot().gaze.eye.x));
});

for (const fallback of [true, false]) for (const event of ['stageResize', 'windowResize']) test((fallback ? 'dormant SVG' : 'scene controller') + ' ' + event + ' remaps a stationary pointer without waiting for a new pointer or blink', async t => {
  const f = await fixture(t, {fallback, resizeObserver: event === 'stageResize'}); f.pointer(340, 180); f.advance(2200);
  assert.equal(f.frames.size, 0, 'the gaze trajectory has genuinely stopped scheduling animation');
  const before = f.guide.snapshot(); assert.ok(before.gaze.target.x > .3); assert.equal(before.productFocus, null);
  f[event]({left: 900}); const after = f.guide.snapshot();
  assert.equal(after.gaze.target.x, -1, 'the saved cursor is remapped immediately to the moved dock');
  assert.deepEqual(after.gaze.eye, before.gaze.eye, 'dock resize retains the displayed eyes');
  assert.deepEqual(after.gaze.head, before.gaze.head, 'dock resize retains the displayed head');
  for (const axis of Object.keys(before.gaze.pose)) assert.equal(after.gaze.pose[axis].position, before.gaze.pose[axis].position, 'no dormant head pose jump');
  if (fallback) assert.deepEqual(after.gaze.velocity, {eye: {x: 0, y: 0}, head: {x: 0, y: 0}}, 'a genuinely dormant sampling gap retires its tiny residual momentum');
  else assert.deepEqual(after.gaze.velocity, before.gaze.velocity, 'a current scene channel retains its velocity');
  assert.ok(f.frames.size > 0, 'existing bounded trajectory is scheduled again');
  assert.equal(after.mannerism.id, before.mannerism.id, 'resize is never a presentation gesture');
  if (event === 'stageResize') assert.equal(f.geometry().stageObserved, true, 'observer follows the actual containing stage');
  f.advance(800); assert.ok(f.guide.snapshot().gaze.eye.x < -.98);
  assert.ok(f.guide.snapshot().gaze.pose.headYaw.position < -.095);
});

test('resize retains in-flight velocities, respects pause/visibility/reduced motion and ignores late disposed callbacks', async t => {
  const f = await fixture(t, {fallback: true}); f.pointer(650, 100); f.advance(96); const before = f.guide.snapshot();
  assert.ok(before.gaze.velocity.eye.x > 0); f.stageResize({left: 900}); samePose(before, f.guide.snapshot(), 'moving stage resize');
  f.frame(); assert.ok(Math.abs(f.guide.snapshot().gaze.velocity.eye.x - before.gaze.velocity.eye.x) <= 80 * .016 + 1e-9);
  for (const [stop, start] of [[() => f.guide.setPaused(true), () => f.guide.setPaused(false)], [() => f.hide(true), () => f.hide(false)], [() => f.intersect(false), () => f.intersect(true)]]) {
    stop(); const stopped = f.guide.snapshot(); f.stageResize({left: 100}); f.windowResize({top: 200}); samePose(stopped, f.guide.snapshot(), 'guarded dock resize'); assert.equal(f.frames.size, 0);
    start(); f.advance(64); assert.deepEqual(f.guide.snapshot().gaze.eye, {x: 0, y: 0});
  }
  f.guide.setReducedMotion(true); f.pointer(340, 200); f.stageResize({left: 900}); const reduced = f.guide.snapshot();
  assert.deepEqual(reduced.gaze.eye, reduced.gaze.target); assert.equal(f.frames.size, 0, 'reduced motion receives a static remap');
  f.guide.destroy(); const disposed = f.guide.snapshot(); assert.equal(f.geometry().disconnected, true);
  f.stageResize({left: 100}); f.windowResize({left: 300}); samePose(disposed, f.guide.snapshot(), 'late disposed resize'); assert.equal(f.frames.size, 0);
});

for (const fallback of [true, false]) test((fallback ? 'SVG' : 'scene controller') + ' latest input stays current at 33ms while authored render RAF is about1Hz; burst resume stays bounded', async t => {
  const f = await fixture(t, {fallback}); let previous = f.guide.snapshot();
  for (let index = 0; index < 90; index++) {
    f.elapse(33); const beforeInput = f.guide.snapshot(); f.guide.clearFocus(); f.guide.focusProduct({x: index % 2 ? -.9 : .9, y: .5, source: 'hover'});
    const beforePointer = f.guide.snapshot(); f.pointer(index % 2 ? 300 : 340, 180, index % 2 ? 'two' : 'one');
    samePose(beforePointer, f.guide.snapshot(), 'pointer at the same event clock');
    const next = f.guide.snapshot(); assert.equal(next.gaze.source, 'pointer'); assert.ok(Math.abs(next.gaze.eye.x - previous.gaze.eye.x) <= 8 * .033 + 1e-9);
    assert.ok(Math.abs(next.gaze.pose.headYaw.position - previous.gaze.pose.headYaw.position) <= .42 * .033 + 1e-9);
    assert.equal(next.mannerism.id, 0); assert.equal(beforeInput.gaze.pose.headYaw.position, previous.gaze.pose.headYaw.position);
    if (index % 30 === 29) f.frame(10); previous = f.guide.snapshot();
  }
  const held = f.guide.snapshot(); f.frame(1000); const stale = f.guide.snapshot();
  assert.deepEqual(stale.gaze.eye, held.gaze.eye); assert.deepEqual(stale.gaze.velocity, {eye: {x: 0, y: 0}, head: {x: 0, y: 0}});
  f.guide.setPaused(true); f.elapse(800); for (let index = 0; index < 25; index++) f.pointer(index % 2 ? 100 : 650, 100);
  f.guide.setPaused(false); assert.deepEqual(f.guide.snapshot().gaze.eye, {x: 0, y: 0}, 'paused input is never replayed');
  f.elapse(33); f.pointer(650, 100); const fresh = f.guide.snapshot();
  assert.ok(fresh.gaze.eye.x > 0 && fresh.gaze.eye.x < .1, 'fresh input starts an acceleration-bounded response');
  assert.ok(Math.abs(fresh.gaze.pose.headYaw.position) < .42 * .033);
});

test('emotion changes and all head axes stay bounded while speaking and cursor targets reverse', async t => {
  const f = await fixture(t); f.guide.setState('speaking'); let previous = f.guide.snapshot();
  for (let index = 0; index < 160; index++) {
    f.guide.setEmotion(index % 3 ? 'curious' : 'warm');
    f.guide.lookAt(index % 2 ? -1 : 1, index % 2 ? 1 : -1, false);
    f.guide.setExpression({kind: index % 2 ? 'inquiry' : 'support', intensity: .9});
    samePose(previous, f.guide.snapshot(), 'same-clock expression and lookAt');
    const next = f.frame();
    for (const axis of ['headYaw', 'headPitch', 'headRoll']) {
      assert.ok(Math.abs(next.gaze.pose[axis].velocity) <= .42 + 1e-9);
      assert.ok(Math.abs(next.gaze.pose[axis].velocity - previous.gaze.pose[axis].velocity) <= 3.2 * .016 + 1e-9);
      assert.ok(Math.abs(next.gaze.pose[axis].position - previous.gaze.pose[axis].position) <= .42 * .016 + 1e-9);
    }
    assert.equal(f.pose().mouthOpen, 0); previous = next;
  }
});

test('stale frame gaps discard momentum without catching up or accepting backwards or invalid clocks', async t => {
  const f = await fixture(t); f.pointer(650, 100); f.advance(64); const before = f.guide.snapshot();
  f.jump(1200); const after = f.guide.snapshot();
  assert.deepEqual(after.gaze.eye, before.gaze.eye); assert.deepEqual(after.gaze.head, before.gaze.head);
  assert.deepEqual(after.gaze.velocity, {eye: {x: 0, y: 0}, head: {x: 0, y: 0}});
  for (const [axis, channel] of Object.entries(after.gaze.pose)) {assert.equal(channel.position, before.gaze.pose[axis].position); assert.equal(channel.velocity, 0);}
  f.at(NaN); samePose(after, f.guide.snapshot(), 'invalid frame clock');
  f.at(999); samePose(after, f.guide.snapshot(), 'backwards frame clock');
  f.at(2280); f.pointer(0, 350); f.frame(); assert.ok(Number.isFinite(f.guide.snapshot().gaze.eye.x));
});

test('pause, hidden and offscreen retire cursor, card, velocity and pending hover dwell; resume waits for fresh input', async t => {
  const f = await fixture(t);
  for (const [stop, start] of [[() => f.guide.setPaused(true), () => f.guide.setPaused(false)], [() => f.hide(true), () => f.hide(false)], [() => f.intersect(false), () => f.intersect(true)]]) {
    f.pointer(650, 100); f.guide.focusProduct({x: -.8, y: -.3, source: 'hover'}); f.advance(64); stop();
    const stopped = f.guide.snapshot(); assert.deepEqual(stopped.gaze.eye, {x: 0, y: 0}); assert.deepEqual(stopped.gaze.velocity, {eye: {x: 0, y: 0}, head: {x: 0, y: 0}}); assert.equal(stopped.productFocus, null); assert.equal(f.frames.size, 0);
    f.pointer(0, 350); start(); f.advance(600); assert.deepEqual(f.guide.snapshot().gaze.eye, {x: 0, y: 0});
    f.pointer(650, 100); f.advance(90); assert.ok(f.guide.snapshot().gaze.eye.x > .2, 'a new cursor is noticed within90ms');
  }
});

test('reduced-motion direction is static and no fallback CSS adds a separate head animation or focus rotation', async t => {
  const f = await fixture(t, {fallback: true, reduced: true}); f.pointer(650, 100);
  assert.deepEqual(f.guide.snapshot().gaze.eye, f.guide.snapshot().gaze.target); assert.equal(f.frames.size, 0);
  f.guide.focusProduct({x: -.8, y: .3, source: 'presentation'}); const before = f.guide.snapshot(); f.advance(1000); samePose(before, f.guide.snapshot(), 'reduced direction');
  const css = readFileSync(require.resolve('../../brites-concierge-avatar.css'), 'utf8');
  assert.doesNotMatch(css, /--brites-focus-head|@keyframes britesRobot(?:Hello|Listen|Focus|Nod|Reassure)/);
  assert.match(css, /\.brites-avatar__head\{[^}]*transition:none;animation:none/);
});
