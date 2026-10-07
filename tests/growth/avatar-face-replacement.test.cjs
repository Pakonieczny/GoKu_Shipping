'use strict';

// Execute production geometry, material declarations and articulation with real
// Three.js classes and a synthetic renderer. These certify CPU contracts only;
// they do not certify WebGL pixels, real shadows, frame-rate or voice capture.
const test = require('node:test'), assert = require('node:assert/strict');
const fs = require('node:fs'), vm = require('node:vm');
const {JSDOM} = require('jsdom'), THREE = require('three');
const avatar = require('../../brites-concierge-avatar.js');
const source = fs.readFileSync(require.resolve('../../brites-concierge-avatar-scene.mjs'), 'utf8');

function harness(t) {
  const dom = new JSDOM('<div id="stage"></div>', {pretendToBeVisual: true}), win = dom.window, stage = win.document.getElementById('stage');
  let renderer, resizeCallback, time = 0, box = {width: 480, height: 440};
  Object.defineProperty(win.performance, 'now', {value: () => time});
  stage.getBoundingClientRect = () => box;
  win.ResizeObserver = class {constructor(callback) {resizeCallback = callback;} observe() {} disconnect() {}};
  win.HTMLCanvasElement.prototype.getContext = type => type === '2d' ? {createImageData(width, height) {return {data: new Uint8ClampedArray(width * height * 4)};}, putImageData() {}} : null;
  class SyntheticRenderer {
    constructor() {renderer = this; this.domElement = win.document.createElement('canvas'); this.shadowMap = {}; this.capabilities = {getMaxAnisotropy: () => 1}; this.info = {autoReset: true, render: {calls: 0, triangles: 0}, reset() {}};}
    setClearColor() {}
    setPixelRatio(value) {this.ratio = value;}
    getPixelRatio() {return this.ratio;}
    setSize() {}
    setAnimationLoop(callback) {this.callback = callback;}
    render(scene, camera) {this.scene = scene; this.camera = camera;}
    dispose() {}
    forceContextLoss() {}
  }
  class SyntheticPmrem {fromEquirectangular(texture) {return {texture, dispose() {}};} dispose() {}}
  const module = {exports: {}};
  vm.runInNewContext(source.replace(/^import [^\n]+\n/gm, '').replace(/export (const|function) /g, '$1 ') + '\nmodule.exports={createAvatarScene,AVATAR_SCENE_DECLARATIONS};', {module, THREE: {...THREE, WebGLRenderer: SyntheticRenderer, PMREMGenerator: SyntheticPmrem}}, {filename: 'avatar-face-replacement-production-scene.cjs'});
  const engine = module.exports.createAvatarScene({container: stage, quality: {...avatar.qualityFor({width: 1440, bloom: false}), textureSize: 16}, onFrame: value => avatar.poseFor({time: value, state: 'idle'})});
  t.after(() => {engine.destroy(); win.close();});
  const h = {
    engine, declarations: module.exports.AVATAR_SCENE_DECLARATIONS,
    get scene() {return renderer.scene;}, get camera() {return renderer.camera;},
    pose(fields = {}) {engine.render({...avatar.poseFor({state: fields.state || 'idle', time: time / 1000, ...fields}), ...fields}, true); renderer.scene.updateMatrixWorld(true);},
    get(name) {return renderer.scene.getObjectByName(name);},
    resize(width, height) {box = {width, height}; resizeCallback();},
    advance(milliseconds) {time = milliseconds;},
    motion(fields) {engine.setMotion(fields);}
  };
  h.pose(); return h;
}
const eyeNames = ['expression-eye-left', 'expression-eye-right'];
const faceNames = [...eyeNames, 'expression-brow-left', 'expression-brow-right', 'expression-cheek-left', 'expression-cheek-right', 'expression-smile-glyph', 'expression-heart-left', 'expression-heart-right'];
function geometrySize(mesh) {mesh.geometry.computeBoundingBox(); return mesh.geometry.boundingBox.getSize(new THREE.Vector3());}
function validGeometry(mesh) {
  for (const [name, attribute] of Object.entries(mesh.geometry.attributes)) assert.ok([...attribute.array].every(Number.isFinite), mesh.name + ' ' + name + ' is finite');
  assert.ok(mesh.geometry.attributes.position.count > 0, mesh.name + ' real vertices');
  if (mesh.geometry.index) assert.ok([...mesh.geometry.index.array].every(index => index >= 0 && index < mesh.geometry.attributes.position.count), mesh.name + ' valid indices');
  for (const vector of [mesh.position, mesh.rotation, mesh.scale]) for (const axis of ['x', 'y', 'z']) assert.ok(Number.isFinite(vector[axis]), mesh.name + ' ' + axis + ' transform is finite');
}

test('the new face contains two filled light ribbons and no old ring or foreground shutters', t => {
  const h = harness(t), visor = h.get('original-wide-visor'), bezel = h.get('original-visor-bezel'), pair = h.get('expression-eye-pair');
  assert.ok(geometrySize(visor).x / geometrySize(visor).y > 1.4, 'visor is visibly wider than tall');
  assert.equal(visor.geometry.type, 'ExtrudeGeometry'); assert.equal(bezel.geometry.type, 'ExtrudeGeometry');
  assert.equal(h.get('original-pebble-shell').geometry.type, 'SphereGeometry', 'the continuous design has a smooth oval helmet rather than a square monitor');
  assert.equal(h.get('expression-upper-shutter'), undefined); assert.equal(h.get('expression-lower-shutter'), undefined);
  const visibleMeshes = []; pair.traverse(part => {if (part.isMesh && part.visible) visibleMeshes.push(part);});
  assert.equal(visibleMeshes.length, 2); assert.deepEqual(visibleMeshes.map(part => part.name), eyeNames);
  for (const part of visibleMeshes) {assert.equal(part.geometry.type, 'ExtrudeGeometry'); validGeometry(part); assert.ok(geometrySize(part).x > .4); assert.ok(geometrySize(part).z > .03);}
  assert.equal(h.get('retired-aperture-ornament').children.length, 0, 'retired ring contains no mesh');
  assert.equal(h.engine.snapshot().character.digitalEyes, 2);
  assert.deepEqual({...h.engine.snapshot().character.faceGeometry}, {lightRibbons: 2, brows: 2, shutters: 0, cheekFacets: 2, smileGlyph: true, signalMarkers: 3, appreciationGlyphs: 2, speechBars: 5, speechMouth: true});
});

test('opaque old geometry cannot cover either replacement eye along the viewing ray', t => {
  const h = harness(t), head = h.get('articulated-expression-head'), opaque = [];
  head.traverse(part => {if (part.isMesh && part.visible && !part.material.transparent) opaque.push(part);});
  for (const name of eyeNames) {
    const eye = h.get(name), target = eye.localToWorld(new THREE.Vector3(0, 0, .02)), direction = target.clone().sub(h.camera.position).normalize(), ray = new THREE.Raycaster(h.camera.position, direction);
    const hits = ray.intersectObjects(opaque, false); assert.ok(hits.length > 0, name + ' has visible triangles'); assert.equal(hits[0].object, eye, name + ' is the foremost opaque face');
  }
  for (const name of faceNames) {const part = h.get(name); if (part.visible) assert.ok(part.getWorldPosition(new THREE.Vector3()).z > h.get('original-wide-visor').getWorldPosition(new THREE.Vector3()).z, name + ' in front of visor');}
});

test('all seven expressions alter real silhouettes and cues independently of colour', t => {
  const h = harness(t), signatures = new Set();
  for (const expression of h.declarations.expressions) {
    h.pose({state: 'idle', emotion: 'neutral', faceExpression: expression, ...avatar.FACE_EXPRESSIONS[expression], eyeOpen: 1, eyeScaleX: 1, eyeScaleY: 1, eyeDeformation: 0, lidClosure: avatar.FACE_EXPRESSIONS[expression].eyeSmile * .18, heart: 0});
    const eyeGeometry = eyeNames.map(name => Array.from(h.get(name).geometry.attributes.position.array));
    const cues = ['expression-brow-left', 'expression-brow-right', 'expression-cheek-left', 'expression-smile-glyph'].map(name => {const part = h.get(name); return [part.position.toArray(), part.rotation.toArray(), part.scale.toArray()];});
    signatures.add(JSON.stringify([eyeGeometry, cues]));
    for (const name of faceNames) validGeometry(h.get(name));
    assert.equal(h.engine.snapshot().character.faceExpression, expression);
  }
  assert.equal(signatures.size, 7, 'expression geometry is not seven colours on one shape');
});

test('blink closes both ribbons and event-bound appreciation replaces both eyes with hearts', t => {
  const h = harness(t); h.pose({eyeOpen: 1, eyeSmile: 0, lidClosure: 0, heart: 0});
  const openHeight = eyeNames.map(name => geometrySize(h.get(name)).y);
  h.pose({eyeOpen: .035, eyeSmile: 0, lidClosure: 1, heart: 0});
  eyeNames.forEach((name, index) => assert.ok(geometrySize(h.get(name)).y < openHeight[index] * .05, name + ' closes'));
  h.pose({emotion: 'appreciated', appreciationElapsed: .35});
  for (const name of eyeNames) assert.equal(h.get(name).visible, false);
  for (const name of ['expression-heart-left', 'expression-heart-right']) {assert.equal(h.get(name).visible, true); validGeometry(h.get(name));}
  h.pose({emotion: 'appreciated', appreciationElapsed: 1.7});
  for (const name of eyeNames) assert.equal(h.get(name).visible, true);
  for (const name of ['expression-heart-left', 'expression-heart-right']) assert.equal(h.get(name).visible, false);
});

test('gaze moves the paired eyes together and negative head pitch projects upward', t => {
  const h = harness(t); h.pose({gazeX: 0, gazeY: 0, headPitch: 0, headYaw: 0, headRoll: 0});
  const pair = h.get('expression-eye-pair'), neutralAnchor = h.engine.gazeAnchor(), versions = eyeNames.map(name => h.get(name).geometry.attributes.position.version);
  h.pose({gazeX: .1, gazeY: .07, headPitch: -.12, headYaw: 0, headRoll: 0});
  assert.equal(pair.position.x, .1); assert.ok(pair.position.y > .022); assert.ok(h.engine.gazeAnchor().y < neutralAnchor.y);
  h.pose({gazeX: -.1, gazeY: -.07, headPitch: .12, headYaw: 0, headRoll: 0});
  assert.equal(pair.position.x, -.1); assert.ok(pair.position.y < .022); assert.ok(h.engine.gazeAnchor().y > neutralAnchor.y);
  assert.deepEqual(eyeNames.map(name => h.get(name).geometry.attributes.position.version), versions, 'pointer following does not rebuild unchanged geometry');
  for (const [width, height] of [[180, 440], [390, 280], [980, 440]]) {
    h.resize(width, height); h.pose({headPitch: 0, headYaw: 0, headRoll: 0});
    const anchor = h.engine.gazeAnchor(), projected = h.get('articulated-expression-head').localToWorld(new THREE.Vector3(0, .112, .71)).project(h.camera);
    assert.ok(anchor.x > 0 && anchor.x < 1); assert.ok(anchor.y > 0 && anchor.y < 1);
    assert.ok(Math.abs(anchor.x - (projected.x + 1) / 2) < 1e-9); assert.ok(Math.abs(anchor.y - (1 - projected.y) / 2) < 1e-9);
  }
});

test('speech marks respond only to measured output and a held level keeps a held silhouette', t => {
  const h = harness(t), barNames = Array.from({length: 5}, (_, index) => 'expression-speech-bar-' + index);
  h.pose({state: 'speaking', speechEnergy: 0}); for (const name of barNames) assert.equal(h.get(name).visible, false);
  h.pose({state: 'listening', speechEnergy: 1}); for (const name of barNames) assert.equal(h.get(name).visible, false);
  h.pose({state: 'speaking', speechEnergy: .2}); const quietHeight = h.get(barNames[2]).scale.y;
  h.pose({state: 'speaking', speechEnergy: .8}); assert.ok(h.get(barNames[2]).scale.y > quietHeight);
  for (const name of barNames) assert.equal(h.get(name).visible, true);
  const capture = () => JSON.stringify([eyeNames.map(name => Array.from(h.get(name).geometry.attributes.position.array)), barNames.map(name => h.get(name).scale.toArray())]);
  const fields = {state: 'speaking', speechEnergy: .8, eyeOpen: 1, eyeSmile: .15, eyeDeformation: .075, eyeScaleX: 1, eyeScaleY: 1, lidClosure: .02, heart: 0};
  h.advance(1000); h.pose(fields); const first = capture(); h.advance(11000); h.pose(fields); assert.equal(capture(), first, 'time alone does not invent speech');
  h.motion({active: true, reducedMotion: true}); h.pose(fields); const reduced = capture(); h.advance(20000); h.pose(fields); assert.equal(capture(), reduced, 'reduced-motion geometry remains stable');
});

test('malformed expression and gaze numbers cannot poison geometry, lights or transforms', t => {
  const h = harness(t); h.pose({eyeOpen: NaN, eyeScaleX: Infinity, eyeScaleY: -Infinity, eyeDeformation: NaN, gazeX: NaN, gazeY: Infinity, faceBrowLift: Infinity, faceBrowTilt: NaN, eyeSmile: -99, cheekGlow: 99, smileCurve: NaN, faceSignal: -Infinity, lidClosure: 99, heart: Infinity, speechEnergy: NaN, lightPulse: Infinity, bodyYaw: Infinity, bodyRoll: NaN, headRoll: NaN});
  for (const name of faceNames) validGeometry(h.get(name));
  h.scene.traverse(part => {if (part.isLight) assert.ok(Number.isFinite(part.intensity));});
  assert.ok(h.get('expression-cheek-left').scale.x <= 1.1); assert.equal(h.get('expression-eye-pair').position.x, 0);
  for (const part of [h.get('supported-upper-body'), h.get('articulated-expression-head')]) for (const axis of ['x', 'y', 'z']) assert.ok(Number.isFinite(part.rotation[axis]));
});

test('paired light materials bypass tone mapping while the ceramic and metal retain their PBR maps', t => {
  const h = harness(t);
  for (const name of eyeNames) {assert.equal(h.get(name).material.isMeshBasicMaterial, true); assert.equal(h.get(name).material.toneMapped, false);}
  const ceramic = h.get('original-pebble-shell').material, visor = h.get('original-wide-visor').material, gold = h.get('original-visor-bezel').material;
  assert.equal(ceramic.isMeshPhysicalMaterial, true); assert.ok(ceramic.map && ceramic.normalMap && ceramic.bumpMap); assert.ok(ceramic.roughness >= .6);
  assert.equal(visor.isMeshPhysicalMaterial, true); assert.ok(visor.roughness >= .45); assert.ok(gold.metalness > .7 && gold.roughnessMap);
  assert.equal(h.engine.snapshot().textures.length, 6); assert.equal(h.engine.snapshot().shadow.enabled, true); assert.equal(h.engine.snapshot().finish.bloom, 'disabled pending GPU verification');
});

test('the oval helmet remains connected and the continuous body stays planted on the platform', t => {
  const h = harness(t), head = new THREE.Box3().setFromObject(h.get('original-pebble-shell')), neck = new THREE.Box3().setFromObject(h.get('supported-neck'));
  assert.ok(head.min.y < neck.max.y, 'neck reaches inside the ceramic shell');
  const platformTop = new THREE.Box3().setFromObject(h.get('grounding-platform')).max.y;
  for (const fields of [{state: 'idle', bob: -.4, stanceScale: .94, bodyRoll: -.3, bodyYaw: -.3, lean: -.3}, {state: 'speaking', level: 1, bob: .4, stanceScale: 1.06, bodyRoll: .3, bodyYaw: .3, lean: .3}, {state: 'success', emotion: 'celebrate', bob: .14, stanceScale: 1.04}]) {
    h.pose(fields);
    assert.ok(Math.abs(new THREE.Box3().setFromObject(h.get('sculpted-porcelain-torso')).min.y - platformTop) < 1e-6);
  }
});
