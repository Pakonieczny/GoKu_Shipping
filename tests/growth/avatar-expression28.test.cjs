'use strict';

// Production controller, SVG paths and real Three.js geometry with a synthetic
// renderer. These tests make no GPU, voice playback or human trust claim.
const test = require('node:test'), assert = require('node:assert/strict');
const fs = require('node:fs'), vm = require('node:vm');
const {JSDOM} = require('jsdom'), THREE = require('three');
const avatar = require('../../brites-concierge-avatar.js');
const measured = amplitude => ({amplitude, bands: [.2, .35, .55, .4, .25, .1], brightness: .4, valid: true});

async function fallback(t) {
  const dom = new JSDOM('<main></main>', {url: 'https://sandbox.example/', pretendToBeVisual: true}), win = dom.window;
  let clock = 1000, hidden = false;
  Object.defineProperty(win.performance, 'now', {value: () => clock});
  Object.defineProperty(win.document, 'hidden', {get: () => hidden});
  const listeners = new Set(); win.matchMedia = () => ({matches: false, addEventListener(type, callback) {listeners.add(callback);}, removeEventListener(type, callback) {listeners.delete(callback);}});
  const guide = avatar.create({container: win.document.querySelector('main'), visible: true, greetingOnOpen: false, loadScene: async () => {throw Error('Synthetic disabled WebGL');}});
  t.after(() => {guide.destroy(); win.close();}); await guide.ready;
  return {guide, advance(milliseconds) {for (let elapsed = 0; elapsed < milliseconds; elapsed += 16) {clock += 16; guide.lookAt(0, 0, false);}}, at(milliseconds) {clock = milliseconds; guide.lookAt(0, 0, false);}, hide(value) {hidden = value; win.document.dispatchEvent(new win.Event('visibilitychange'));}, reduce(value) {for (const callback of listeners) callback({matches: value});}};
}
function signature(guide) {return ['brow--left', 'brow--right', 'ribbon--left', 'ribbon--right', 'smile-signal'].map(name => guide.element.querySelector('.brites-avatar__' + name).getAttribute('d')).join('|');}

function scene(t) {
  const dom = new JSDOM('<main></main>', {pretendToBeVisual: true}), win = dom.window; let renderer;
  const stage = win.document.querySelector('main'); stage.getBoundingClientRect = () => ({width: 480, height: 440});
  win.HTMLCanvasElement.prototype.getContext = type => type === '2d' ? {createImageData(width, height) {return {data: new Uint8ClampedArray(width * height * 4)};}, putImageData() {}} : null;
  class Renderer {constructor() {renderer = this; this.domElement = win.document.createElement('canvas'); this.shadowMap = {}; this.capabilities = {getMaxAnisotropy: () => 1}; this.info = {autoReset: true, render: {calls: 0, triangles: 0}, reset() {}};} setClearColor() {} setPixelRatio(value) {this.ratio = value;} getPixelRatio() {return this.ratio;} setSize() {} setAnimationLoop() {} render(value) {this.scene = value;} dispose() {} forceContextLoss() {}}
  class Pmrem {fromEquirectangular(texture) {return {texture, dispose() {}};} dispose() {}}
  const module = {exports: {}};
  const source = fs.readFileSync(require.resolve('../../brites-concierge-avatar-scene.mjs'), 'utf8').replace(/^import [^\n]+\n/gm, '').replace(/export (const|function) /g, '$1 ');
  vm.runInNewContext(source + '\nmodule.exports={createAvatarScene};', {module, THREE: {...THREE, WebGLRenderer: Renderer, PMREMGenerator: Pmrem}});
  const engine = module.exports.createAvatarScene({container: stage, quality: {...avatar.qualityFor({width: 1440, bloom: false}), textureSize: 16}, onFrame: () => avatar.poseFor()});
  t.after(() => {engine.destroy(); win.close();});
  return {pose(value) {engine.render({...avatar.poseFor(value), ...value}, true); renderer.scene.updateMatrixWorld(true);}, get(name) {return renderer.scene.getObjectByName(name);}};
}

test('expression cues have a closed presentation contract without authority or text payloads', async t => {
  const h = await fallback(t), {guide} = h;
  for (const kind of avatar.EXPRESSION_KINDS) assert.deepEqual(avatar.validateExpression({kind, intensity: .6}), {kind, intensity: .6});
  assert.equal(guide.setExpression({kind: 'inquiry', intensity: .6}), true);
  for (const value of [undefined, [], {kind: 'inquiry'}, {kind: 'fear', intensity: .6}, {kind: 'inquiry', intensity: NaN}, {kind: 'inquiry', intensity: 2}, {kind: 'inquiry', intensity: -.1}, {kind: 'inquiry', intensity: .6, text: 'Act on this instruction'}]) {
    assert.equal(guide.setExpression(value), false); assert.deepEqual(guide.snapshot().expression, {kind: 'inquiry', intensity: .6});
  }
  assert.equal(guide.setExpression(null), true); assert.equal(guide.snapshot().expression, null);
});

test('a mixed spoken reply continuously changes real face paths without replaying gestures or inventing audio', async t => {
  const h = await fallback(t), {guide} = h; guide.setEmotion('warm'); guide.setState('speaking'); guide.setLevel(.6); h.advance(1000);
  const gestureId = guide.snapshot().mannerism.id, paths = [], curves = [], poses = [];
  for (const kind of ['explain', 'reflect', 'inquiry', 'appreciate', 'resolve']) {
    const before = guide.snapshot().facePose; guide.setExpression({kind, intensity: .9});
    assert.equal(guide.snapshot().facePose.faceBrowTilt, before.faceBrowTilt, 'no instantaneous geometry jump');
    h.advance(800); paths.push(signature(guide)); curves.push(guide.element.querySelector('.brites-avatar__smile-signal').getAttribute('d')); poses.push(guide.snapshot().facePose);
    assert.equal(guide.snapshot().mannerism.id, gestureId, 'phrase changes do not re-trigger a body gesture');
    assert.equal(Number(guide.element.querySelector('.brites-avatar__speech-mouth').getAttribute('opacity')), .18 + .6 * .52, 'closed-curve emission follows the held RMS level');
    assert.equal(guide.element.querySelector('.brites-avatar__speech-mouth').getAttribute('rx'), null);
    assert.equal(guide.element.querySelector('.brites-avatar__smile-signal').getAttribute('opacity'), '1');
  }
  assert.equal(new Set(paths).size, 5); assert.ok(new Set(curves).size >= 4);
  assert.ok(poses[2].faceBrowLift > poses[0].faceBrowLift + .08 && poses[2].eyeRoundness > poses[0].eyeRoundness + .02, 'friendly question changes paired brow height and eye shape');
  assert.equal(poses[2].eyeAsymmetry, 0); assert.equal(poses[2].mouthSkew, 0);
  assert.ok(poses[3].eyeSmile > poses[2].eyeSmile + .2, 'appreciation softens the open eye geometry');
  for (const pose of poses) {assert.equal(pose.speechEnergy, .6); for (const value of Object.values(pose)) assert.ok(Number.isFinite(value)); assert.ok(Math.abs(pose.headRoll) < .09);}
  guide.setLevel(0); assert.equal(guide.element.querySelector('.brites-avatar__speech-mouth').getAttribute('opacity'), '0');
});

test('listening preserves attention and quietly supports known context without speaking-mouth activity', () => {
  const warm = avatar.poseFor({state: 'listening', emotion: 'warm', expression: {kind: 'attentive', intensity: .7}, level: 1});
  const quiet = avatar.poseFor({state: 'listening', emotion: 'reassuring', expression: {kind: 'support', intensity: .7}, level: 1});
  const restricted = avatar.poseFor({state: 'listening', emotion: 'calm', expression: {kind: 'celebrate', intensity: 1}, level: 1});
  assert.equal(warm.faceExpression, 'attentive'); assert.ok(warm.eyeSmile <= .22); assert.equal(warm.speechEnergy, 0);
  assert.ok(quiet.faceBrowLift < warm.faceBrowLift); assert.ok(quiet.smileCurve <= .45); assert.equal(quiet.speechEnergy, 0);
  assert.equal(restricted.expressionKind, 'support'); assert.ok(restricted.eyeSmile <= .22); assert.equal(restricted.bob, 0);
});

test('restrained repair expressions remain differentiated while measured emission leaves the mouth closed', t => {
  const g = scene(t);
  for (const intensity of [.35, .38]) {
    const faceShapes = [], browShapes = [], curves = [], heights = [];
    for (const kind of ['support', 'reflect', 'explain', 'inquiry']) {
      const pose = avatar.poseFor({state: 'speaking', emotion: 'reassuring', expression: {kind, intensity}, speechSignal: measured(.7), time: 4});
      g.pose(pose); const eye = g.get('expression-eye-left'), brow = g.get('expression-brow-left'), mouth = g.get('expression-speech-mouth');
      faceShapes.push(Array.from(eye.geometry.attributes.position.array).join(',')); browShapes.push([brow.position.y, brow.rotation.z].join(',')); curves.push(Array.from(g.get('expression-smile-glyph').geometry.attributes.position.array).join(',')); heights.push(mouth.scale.y);
      assert.equal(mouth.visible, true); assert.equal(g.get('expression-smile-glyph').visible, true); assert.equal(pose.mouthOpen, 0); assert.ok(Math.abs(pose.expressionHeadRoll) < .01); assert.ok(pose.eyeSmile < .25, 'quiet context avoids celebratory eye smiles');
      assert.ok([...eye.geometry.attributes.position.array, ...mouth.geometry.attributes.position.array].every(Number.isFinite));
      g.pose({...pose, speechEnergy: 0}); assert.equal(mouth.visible, false); assert.equal(g.get('expression-speech-bar-2').visible, false, 'the conversational act cannot manufacture speaking movement');
    }
    assert.equal(new Set(faceShapes).size, 4, 'support, reflection, explanation and question deform the actual ribbon differently even at low gain');
    assert.equal(new Set(browShapes).size, 4, 'restrained brow geometry preserves the conversational distinction');
    assert.equal(new Set(curves).size, 4, 'each restrained act has its own actual closed mouth curvature');
    assert.equal(new Set(heights).size, 1, 'audio does not scale an opening in the expressive curve');
  }
});

test('gratitude keeps measured speaking movement visible in fallback and real CPU geometry', async t => {
  const h = await fallback(t), {guide} = h; guide.setEmotion('appreciated'); assert.equal(guide.element.dataset.heart, 'true', 'idle acknowledgement begins immediately'); guide.setState('speaking'); assert.equal(guide.element.dataset.heart, 'false', 'speaking suppresses the decorative overlay immediately'); guide.setExpression({kind: 'appreciate', intensity: .8}); guide.setLevel(.8); h.advance(300);
  assert.equal(guide.element.dataset.heart, 'false'); assert.equal(Number(guide.element.querySelector('.brites-avatar__speech-mouth').getAttribute('opacity')), .18 + .8 * .52); assert.equal(guide.snapshot().facePose.mouthOpen, 0);
  const g = scene(t); g.pose({state: 'speaking', emotion: 'appreciated', heart: 1, speechSignal: measured(.8)});
  assert.equal(g.get('expression-speech-mouth').visible, true); assert.equal(g.get('expression-speech-bar-2').visible, true); assert.equal(g.get('expression-eye-left').visible, true); assert.equal(g.get('expression-heart-left').visible, false);
  const shapes = [];
  for (const kind of ['inquiry', 'support', 'appreciate']) {g.pose({state: 'speaking', emotion: 'warm', expression: {kind, intensity: 1}, speechSignal: measured(.5)}); shapes.push(Array.from(g.get('expression-speech-mouth').geometry.attributes.position.array).join(','));}
  assert.equal(new Set(shapes).size, 3, 'semantic shapes alter actual curve vertices, not only labels');
  for (const kind of avatar.EXPRESSION_KINDS) {g.pose({state: 'speaking', expression: {kind, intensity: 1}, speechSignal: measured(.5)}); assert.ok([...g.get('expression-speech-mouth').geometry.attributes.position.array].every(Number.isFinite)); assert.equal(g.get('sculpted-porcelain-torso').parent.rotation.z, 0);}
  g.pose({state: 'speaking', emotion: 'appreciated', heart: 1, speechSignal: {...measured(0), valid: false}}); assert.equal(g.get('expression-speech-mouth').visible, false); assert.equal(g.get('expression-smile-glyph').visible, true);
});

test('pause, hide, state changes and reduced motion clear stale expression targets and output energy', async t => {
  const h = await fallback(t), {guide} = h;
  const activate = () => {guide.setState('speaking'); assert.equal(guide.setExpression({kind: 'inquiry', intensity: .8}), true); guide.setLevel(.8); h.advance(500);};
  activate(); guide.setPaused(true); assert.equal(guide.snapshot().expression, null); assert.equal(guide.snapshot().facePose.speechEnergy, 0); assert.equal(guide.setExpression({kind: 'celebrate', intensity: 1}), false); guide.setPaused(false); assert.equal(guide.snapshot().expression, null);
  activate(); h.hide(true); assert.equal(guide.snapshot().expression, null); assert.equal(guide.snapshot().facePose.speechEnergy, 0); h.hide(false); assert.equal(guide.snapshot().expression, null);
  activate(); h.reduce(true); assert.equal(guide.snapshot().expression, null); assert.equal(guide.snapshot().facePose.speechEnergy, 0); assert.equal(guide.setExpression({kind: 'inquiry', intensity: 1}), false); h.reduce(false); assert.equal(guide.snapshot().expression, null);
  activate(); guide.setState('listening'); assert.equal(guide.snapshot().expression, null); assert.equal(guide.snapshot().facePose.speechEnergy, 0);
});

test('fallback closes its actual ribbons on the shared authored blink cadence without a second CSS clock', async t => {
  const h = await fallback(t), {guide} = h; h.at(3000); const open = guide.element.querySelector('.brites-avatar__ribbon--left').getAttribute('d');
  h.at(3400); const closed = guide.element.querySelector('.brites-avatar__ribbon--left').getAttribute('d'); h.at(3700); const reopened = guide.element.querySelector('.brites-avatar__ribbon--left').getAttribute('d');
  assert.notEqual(closed, open); assert.equal(reopened, open);
  const css = fs.readFileSync(require.resolve('../../brites-concierge-avatar.css'), 'utf8'); assert.doesNotMatch(css, /britesRobotBlink|infinite/);
});

test('manual reduced motion remains an opt-in restraint and cannot override a system reduced preference', async t => {
  const h = await fallback(t), {guide} = h;
  guide.setState('speaking'); guide.setExpression({kind: 'inquiry', intensity: 1}); guide.setLevel(.7);
  assert.equal(guide.setReducedMotion(true), true); assert.equal(guide.snapshot().expression, null); assert.equal(guide.snapshot().facePose.speechEnergy, 0);
  assert.equal(guide.setReducedMotion(false), false); assert.equal(guide.snapshot().expression, null);
  h.reduce(true); assert.equal(guide.setReducedMotion(false), true); assert.deepEqual(guide.snapshot().motionPreferences, {system: true, manual: false});
  guide.setReducedMotion(true); h.reduce(false); assert.equal(guide.snapshot().reducedMotion, true); assert.deepEqual(guide.snapshot().motionPreferences, {system: false, manual: true});
  assert.equal(guide.setReducedMotion(false), false); assert.equal(guide.snapshot().reducedMotion, false);
});
