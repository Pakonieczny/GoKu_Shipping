'use strict';
// Actual Three.js geometry/material construction with a synthetic renderer.
// These inspect authored scene properties, not rendered appearance or FPS.
const test = require('node:test'), assert = require('node:assert/strict');
const fs = require('node:fs'), vm = require('node:vm'), THREE = require('three');
const {JSDOM} = require('jsdom');
const avatar = require('../../brites-concierge-avatar.js');
function fixture(t) {
  const dom = new JSDOM('<div id="mount"></div>', {pretendToBeVisual: true});
  const win = dom.window; let renderer, clock = 0;
  Object.defineProperty(win.performance, 'now', {value: () => clock});
  win.HTMLCanvasElement.prototype.getContext = () => ({createImageData(w, h) {return {data: new Uint8ClampedArray(w * h * 4)};}, putImageData() {}});
  const mount = win.document.getElementById('mount'); mount.getBoundingClientRect = () => ({width: 500, height: 440});
  class Renderer {
    constructor() {renderer = this; this.domElement = win.document.createElement('canvas'); this.shadowMap = {}; this.capabilities = {getMaxAnisotropy: () => 2}; this.info = {reset() {}, render: {calls: 0, triangles: 0}};}
    setPixelRatio(value) {this.ratio = value;} getPixelRatio() {return this.ratio;} setClearColor() {} setSize() {} setAnimationLoop(callback) {this.callback = callback;} render(scene) {this.scene = scene;} dispose() {} forceContextLoss() {}
  }
  class PMREM {fromEquirectangular(texture) {return {texture, dispose() {}};} dispose() {}}
  const source = fs.readFileSync(require.resolve('../../brites-concierge-avatar-scene.mjs'), 'utf8').replace(/^import[^\n]+\n/gm, '').replace(/export (const|function) /g, '$1 ') + '\nmodule.exports={createAvatarScene};';
  const module = {exports: {}}; vm.runInNewContext(source, {module, THREE: {...THREE, WebGLRenderer: Renderer, PMREMGenerator: PMREM}});
  const engine = module.exports.createAvatarScene({container: mount, quality: {...avatar.qualityFor({width: 1440, bloom: false}), textureSize: 16}, onFrame: time => avatar.poseFor({time})});
  engine.setMotion({active: true, reducedMotion: false}); renderer.callback(1000);
  t.after(() => {engine.destroy(); win.close();});
  return {engine, scene: renderer.scene, render(pose) {clock += 1000; engine.render(pose, true);}};
}
test('retired dangling orbits and fine jewellery chain cannot contribute any visible geometry', t => {
  const f = fixture(t);
  for (const name of ['retired-aperture-ornament', 'retired-chain-ornament', 'retired-background-orbit', 'retired-stage-ornament']) {
    const object = f.scene.getObjectByName(name); assert.ok(object); assert.equal(object.visible, false); assert.equal(object.castShadow, false, name);
  }
});
test('warm continuous studio sweep receives real shadow configuration and has finite valid geometry', t => {
  const f = fixture(t), sweep = f.scene.getObjectByName('continuous-studio-sweep');
  assert.ok(sweep.isMesh); assert.equal(sweep.receiveShadow, true); assert.equal(sweep.castShadow, false);
  assert.ok(sweep.material.roughness > .9); assert.equal(sweep.material.metalness, 0);
  const position = sweep.geometry.attributes.position;
  assert.ok(Array.from(position.array).every(Number.isFinite));
  assert.ok(Array.from(sweep.geometry.index.array).every(index => Number.isInteger(index) && index >= 0 && index < position.count));
  assert.ok(new Set(Array.from(position.array).filter((_, index) => index % 3 === 1)).size > 3, 'curved sweep has several heights, not a flat panel');
});
test('satin gold and low normal strength avoid concentrated glitter while retaining PBR textures', t => {
  const f = fixture(t), materials = new Set(); f.scene.traverse(object => {if (object.isMesh) materials.add(object.material);});
  const gold = [...materials].find(material => material.color?.getHexString() === 'b79a69');
  assert.ok(gold.isMeshPhysicalMaterial); assert.ok(gold.roughness >= .65); assert.ok(gold.bumpScale <= .0005); assert.ok(gold.anisotropy <= .2); assert.ok(gold.roughnessMap);
  const ceramic = [...materials].find(material => material.normalMap);
  assert.ok(ceramic.normalScale.x <= .06); assert.ok(ceramic.bumpScale <= .001); assert.ok(ceramic.map);
});
test('character choreography does not oscillate studio key light or restore retired decoration', t => {
  const f = fixture(t); let key; f.scene.traverse(object => {if (object.isSpotLight) key = object;}); const x = key.position.x;
  for (const time of [1, 3, 20, 70]) {f.render(avatar.poseFor({state: 'speaking', time, level: .8})); assert.equal(key.position.x, x); assert.equal(f.scene.getObjectByName('retired-background-orbit').visible, false);}
});
