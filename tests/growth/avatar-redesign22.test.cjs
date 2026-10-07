'use strict';

// Production scene/rig scheduling with a synthetic renderer. No GPU, rendered
// appearance, microphone, audible speech, shadows or FPS is certified here.
const test = require('node:test'), assert = require('node:assert/strict');
const fs = require('node:fs'), vm = require('node:vm');
const {JSDOM} = require('jsdom'), THREE = require('three');
const avatar = require('../../brites-concierge-avatar.js');
const sceneSource = fs.readFileSync(require.resolve('../../brites-concierge-avatar-scene.mjs'), 'utf8');

function sceneHarness(t) {
  const dom = new JSDOM('<div id="stage"></div>', {pretendToBeVisual: true});
  const win = dom.window, stage = win.document.getElementById('stage');
  let clock = 0, renderer, radiance, bloom;
  Object.defineProperty(win.performance, 'now', {value: () => clock});
  win.HTMLCanvasElement.prototype.getContext = type => type === '2d' ? {createImageData(width, height) {return {data: new Uint8ClampedArray(width * height * 4)};}, putImageData() {}} : null;
  stage.getBoundingClientRect = () => ({width: 480, height: 440});
  class SyntheticRenderer {
    constructor() {renderer = this; this.domElement = win.document.createElement('canvas'); this.shadowMap = {}; this.capabilities = {getMaxAnisotropy: () => 1}; this.info = {autoReset: true, render: {calls: 0, triangles: 0}, reset() {}}; this.loopChanges = [];}
    setClearColor() {}
    setPixelRatio(value) {this.ratio = value;}
    getPixelRatio() {return this.ratio;}
    setSize() {}
    setAnimationLoop(callback) {this.callback = callback; this.loopChanges.push(callback);}
    render(scene) {this.scene = scene;}
    dispose() {}
    forceContextLoss() {}
  }
  class SyntheticPmrem {
    constructor() {}
    fromEquirectangular(texture) {radiance = texture; return {texture, dispose() {}};}
    dispose() {}
  }
  class SyntheticComposer {
    constructor(renderer) {this.renderer = renderer; this.passes = [];}
    addPass(pass) {this.passes.push(pass);}
    setSize() {}
    render() {this.renderer.render(this.passes[0].scene);}
    dispose() {}
  }
  class SyntheticRenderPass {constructor(scene) {this.scene = scene;}}
  class SyntheticBloom {constructor(size, strength, radius, threshold) {bloom = {strength, radius, threshold};}}
  const source = sceneSource.replace(/^import [^\n]+\n/gm, '').replace(/export (const|function) /g, '$1 ') + '\nmodule.exports={createAvatarScene};';
  const module = {exports: {}};
  vm.runInNewContext(source, {module, THREE: {...THREE, WebGLRenderer: SyntheticRenderer, PMREMGenerator: SyntheticPmrem}, EffectComposer: SyntheticComposer, RenderPass: SyntheticRenderPass, UnrealBloomPass: SyntheticBloom, OutputPass: class {}}, {filename: 'production-avatar-scene.cjs'});
  const sampled = [];
  const engine = module.exports.createAvatarScene({container: stage, quality: {...avatar.qualityFor({width: 1440}), textureSize: 16}, onFrame(time) {sampled.push(time); return avatar.poseFor({time, state: 'idle'});}});
  t.after(() => {engine.destroy(); win.close();});
  return {engine, sampled, get renderer() {return renderer;}, get radiance() {return radiance;}, get bloom() {return bloom;}, advance(milliseconds) {clock = milliseconds; renderer.callback?.(milliseconds);}};
}

test('production finish preserves the dark visor and removes the broad reflective white veil', t => {
  const h = sceneHarness(t);
  h.engine.setMotion({active: true, reducedMotion: false}); h.advance(1000);
  const materials = new Set(); h.renderer.scene.traverse(object => {if (object.isMesh) materials.add(object.material);});
  const ceramic = [...materials].find(material => material.normalMap);
  assert.ok(ceramic.roughness >= .6);
  assert.ok(ceramic.clearcoat <= .15);
  assert.equal(ceramic.metalness, 0);
  const visor = [...materials].find(material => material.color?.getHexString() === '061322');
  assert.ok(visor.roughness >= .45);
  assert.ok(visor.envMapIntensity <= .35);
  const cover = [...materials].find(material => material.transparent && material.opacity < .05);
  assert.equal(cover.transmission, 0);
  assert.ok(cover.envMapIntensity <= .2);
  assert.equal(cover.depthWrite, false);
  for (const name of ['expression-eye-left', 'expression-eye-right']) {
    const ribbon = h.renderer.scene.getObjectByName(name);
    assert.ok(ribbon?.isMesh); assert.equal(ribbon.visible, true);
    assert.equal(ribbon.geometry.type, 'ExtrudeGeometry');
    assert.equal(ribbon.material.isMeshBasicMaterial, true); assert.equal(ribbon.material.toneMapped, false);
    assert.equal('#' + ribbon.material.color.getHexString(), h.engine.snapshot().character.color, 'both graphic lights retain the current state colour');
  }
  assert.equal(h.renderer.scene.getObjectByName('retired-aperture-ornament').children.length, 0);
  assert.equal(h.engine.snapshot().character.digitalEyes, 2);
  assert.ok(h.bloom.threshold > 2);
  assert.ok(h.bloom.strength < .1);
  assert.ok(h.renderer.toneMappingExposure >= 1 && h.renderer.toneMappingExposure <= 1.2);
});

test('studio lighting uses actual finite HDR radiance independently of the display skybox', t => {
  const h = sceneHarness(t), texture = h.radiance;
  assert.equal(texture.type, THREE.FloatType);
  assert.equal(texture.colorSpace, THREE.LinearSRGBColorSpace);
  assert.equal(texture.mapping, THREE.EquirectangularReflectionMapping);
  let max = 0;
  for (const value of texture.image.data) {assert.ok(Number.isFinite(value)); max = Math.max(max, value);}
  assert.ok(max > 3);
  h.engine.setMotion({active: true, reducedMotion: false}); h.advance(1000);
  assert.notEqual(h.renderer.scene.background, texture);
  assert.equal(h.renderer.scene.background.isColor, true);
  assert.equal(h.engine.snapshot().environment.hdr, true);
  assert.equal(h.engine.snapshot().environment.hdri, false);
});

test('continuous scene animation receives seconds, survives repeated sync and obeys pause or reduced motion', t => {
  const h = sceneHarness(t);
  h.engine.setMotion({active: true, reducedMotion: false});
  assert.equal(h.engine.snapshot().animated, false, 'requested loop alone is not evidence of frames');
  h.advance(1000);
  assert.equal(h.sampled[0], 1);
  assert.equal(h.engine.snapshot().animated, true);
  const changes = h.renderer.loopChanges.length;
  for (let index = 1; index <= 180; index++) {
    h.engine.setMotion({active: true, reducedMotion: false});
    h.advance(1000 + index * (1000 / 60));
  }
  assert.equal(h.renderer.loopChanges.length, changes, 'audio/state sync must not repeatedly reset the RAF callback');
  assert.ok(h.engine.snapshot().frames >= 170, '60Hz timestamps must not be accidentally throttled to half cadence');
  const beforePause = h.engine.snapshot().frames;
  h.engine.setMotion({active: false, reducedMotion: false}); h.advance(9000);
  assert.equal(h.renderer.callback, null);
  assert.equal(h.engine.snapshot().frames, beforePause);
  assert.equal(h.engine.snapshot().animated, false);
  h.engine.setMotion({active: true, reducedMotion: true}); h.advance(10000);
  assert.equal(h.renderer.callback, null);
  assert.equal(h.engine.snapshot().frames, beforePause);
  h.engine.setMotion({active: true, reducedMotion: false}); h.advance(11000);
  assert.equal(h.engine.snapshot().frames, beforePause + 1);
});

test('idle support stays still and speech gestures require measured output and a real phrase onset', () => {
  const first = avatar.poseFor({state: 'idle', time: 1}), later = avatar.poseFor({state: 'idle', time: 7});
  assert.equal(first.headYaw, later.headYaw);
  assert.equal(first.headRoll, later.headRoll);
  assert.equal(first.headPitch, later.headPitch);
  assert.equal(first.bob, 0); assert.equal(later.bob, 0);
  const silent = avatar.poseFor({state: 'speaking', time: 14, elapsed: 14, level: 0});
  const audible = avatar.poseFor({state: 'speaking', time: 14, elapsed: 14, level: 1, speechBeatElapsed: .25});
  assert.ok(audible.mouthOpen > silent.mouthOpen * 5);
  assert.ok(audible.armLiftRight > silent.armLiftRight);
  assert.ok(audible.phraseGesture > 0);
  assert.equal(silent.phraseGesture, 0);
  const hello = avatar.poseFor({state: 'idle', time: .5, mannerism: 'greet', mannerismElapsed: .5});
  assert.ok(Math.abs(hello.helloWave) > .1);
});
