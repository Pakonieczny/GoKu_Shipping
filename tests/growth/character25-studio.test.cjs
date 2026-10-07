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
  for (const name of ['retired-aperture-ornament', 'retired-background-orbit', 'retired-stage-ornament']) {
    const object = f.scene.getObjectByName(name); assert.ok(object); assert.equal(object.visible, false); assert.equal(object.castShadow, false, name);
  }
  assert.equal(f.scene.getObjectByName('retired-chain-ornament'), undefined, 'old jewellery chain is removed entirely');
});
test('hidden continuous studio sweep stays out of the pale frame while a separate floor receives shadows', t => {
  const f = fixture(t), sweep = f.scene.getObjectByName('continuous-studio-sweep');
  assert.ok(sweep.isMesh); assert.equal(sweep.visible, false); assert.equal(sweep.receiveShadow, false); assert.equal(sweep.castShadow, false);
  assert.ok(sweep.material.isMeshBasicMaterial); assert.equal(sweep.material.toneMapped, false);
  const position = sweep.geometry.attributes.position;
  assert.ok(Array.from(position.array).every(Number.isFinite));
  assert.ok(Array.from(sweep.geometry.index.array).every(index => Number.isInteger(index) && index >= 0 && index < position.count));
  assert.ok(new Set(Array.from(position.array).filter((_, index) => index % 3 === 1)).size > 3, 'retained curved sweep has finite construction');
  let receiver; f.scene.traverse(object => {if (object.isMesh && object.material.isShadowMaterial) receiver = object;});
  assert.ok(receiver); assert.equal(receiver.receiveShadow, true); assert.equal(receiver.visible, true); assert.ok(receiver.material.opacity <= .15);
  assert.equal(f.scene.background.isColor,true,'the constant backdrop is separate from PBR lighting');
  // Authored linear RGB separation only; actual WebGL appearance is unverified.
  const luminance=color=>.2126*color.r+.7152*color.g+.0722*color.b;
  const backdrop=f.scene.background,body=f.scene.getObjectByName('original-pebble-shell').material.color;
  assert.ok(luminance(backdrop)>.45 && luminance(backdrop)<.75,'the stage is bright while visibly separated from white ceramic');
  assert.ok((luminance(body)+.05)/(luminance(backdrop)+.05)>=1.5,'authored whole-character contrast survives future palette adjustments');
  assert.ok(backdrop.b>=backdrop.g && backdrop.g>backdrop.r,'the restrained cool backdrop separates warm body materials');
});
test('satin gold and low normal strength avoid concentrated glitter while retaining PBR textures', t => {
  const f = fixture(t), materials = new Set(); f.scene.traverse(object => {if (object.isMesh) materials.add(object.material);});
  const gold = [...materials].find(material => material.color?.getHexString() === 'c8b187');
  assert.ok(gold.isMeshPhysicalMaterial); assert.ok(gold.roughness >= .64); assert.ok(gold.bumpScale <= .0005); assert.ok(gold.roughnessMap);
  const ceramic = [...materials].find(material => material.normalMap);
  assert.ok(ceramic.normalScale.x <= .06); assert.ok(ceramic.bumpScale <= .001); assert.ok(ceramic.map);
});
test('character choreography does not oscillate studio key light or restore retired decoration', t => {
  const f = fixture(t); let key; f.scene.traverse(object => {if (object.isSpotLight) key = object;}); const x = key.position.x;
  for (const time of [1, 3, 20, 70]) {f.render(avatar.poseFor({state: 'speaking', time, level: .8})); assert.equal(key.position.x, x); assert.equal(f.scene.getObjectByName('retired-background-orbit').visible, false);}
});
