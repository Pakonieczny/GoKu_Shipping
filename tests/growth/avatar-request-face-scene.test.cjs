'use strict';

// Execute the actual production geometry and rig against a synthetic renderer.
// These checks certify CPU transforms/geometry only: no WebGL pixels, material
// appearance, real shadows, frame-rate or microphone/audio acceptance.
const test = require('node:test'), assert = require('node:assert/strict');
const fs = require('node:fs'), vm = require('node:vm');
const {JSDOM} = require('jsdom'), THREE = require('three');
const avatar = require('../../brites-concierge-avatar.js');
const source = fs.readFileSync(require.resolve('../../brites-concierge-avatar-scene.mjs'), 'utf8');
const css = fs.readFileSync(require.resolve('../../brites-concierge-avatar.css'), 'utf8');

function harness(t) {
  const dom = new JSDOM('<div id="stage"></div>', {pretendToBeVisual: true});
  const win = dom.window, stage = win.document.getElementById('stage');
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
  vm.runInNewContext(source.replace(/^import [^\n]+\n/gm, '').replace(/export (const|function) /g, '$1 ') + '\nmodule.exports={createAvatarScene,AVATAR_SCENE_DECLARATIONS};', {module, THREE: {...THREE, WebGLRenderer: SyntheticRenderer, PMREMGenerator: SyntheticPmrem}}, {filename: 'avatar-request-production-scene.cjs'});
  const engine = module.exports.createAvatarScene({container: stage, quality: {...avatar.qualityFor({width: 1440, bloom: false}), textureSize: 16}, onFrame: value => avatar.poseFor({time: value, state: 'idle'})});
  t.after(() => {engine.destroy(); win.close();});
  const h = {
    engine, declarations: module.exports.AVATAR_SCENE_DECLARATIONS,
    get scene() {return renderer.scene;}, get camera() {return renderer.camera;},
    pose(fields = {}) {engine.render({...avatar.poseFor({state: fields.state || 'idle', time, ...fields}), ...fields}, true); renderer.scene.updateMatrixWorld(true);},
    get(name) {return renderer.scene.getObjectByName(name);},
    resize(width, height) {box = {width, height}; resizeCallback();},
    advance(milliseconds) {time = milliseconds; renderer.callback?.(milliseconds);}
  };
  h.pose(); return h;
}

test('the real scene has readable faceted face geometry, not only recoloured eye rings', t => {
  const h = harness(t), names = ['expression-brow-left', 'expression-brow-right', 'expression-eye-left', 'expression-eye-right', 'expression-cheek-left', 'expression-cheek-right', 'expression-smile-glyph', 'expression-signal-0', 'expression-signal-1', 'expression-signal-2'];
  for (const name of names) {
    const mesh = h.get(name); assert.ok(mesh?.isMesh, name); assert.equal(mesh.visible, true, name);
    for (const attribute of Object.values(mesh.geometry.attributes)) assert.ok([...attribute.array].every(Number.isFinite), name + ' finite buffer');
    assert.ok(mesh.geometry.attributes.position.count > 0, name + ' real geometry');
    if (mesh.geometry.index) assert.ok([...mesh.geometry.index.array].every(index => index >= 0 && index < mesh.geometry.attributes.position.count), name + ' valid indices');
  }
  const description = h.engine.snapshot().character.faceGeometry;
  assert.deepEqual({...description}, {lightRibbons: 2, brows: 2, shutters: 0, cheekFacets: 2, smileGlyph: true, signalMarkers: 3, appreciationGlyphs: 2, speechBars: 5});
  assert.match(h.declarations.interactionProfile.signal, /brow silhouette/);
});

test('warmth and delight change smile curvature, cheek facets and both eye silhouettes', t => {
  const h = harness(t); h.pose({faceExpression: 'neutral', faceBrowLift: 0, faceBrowTilt: 0, smileCurve: .1, eyeSmile: 0, lidClosure: 0, cheekGlow: .1});
  const neutral = {smile: h.get('expression-smile-glyph').scale.y, cheek: h.get('expression-cheek-left').scale.x, eyes: ['expression-eye-left','expression-eye-right'].map(name=>h.get(name).geometry.attributes.position.array.slice())};
  h.pose({faceExpression: 'warm', faceBrowLift: .2, faceBrowTilt: 0, smileCurve: .75, eyeSmile: .7, lidClosure: .098, cheekGlow: .75});
  assert.ok(h.get('expression-smile-glyph').scale.y > neutral.smile * 2);
  assert.ok(h.get('expression-cheek-left').scale.x > neutral.cheek);
  for(const [i,name] of ['expression-eye-left','expression-eye-right'].entries())assert.notDeepEqual(h.get(name).geometry.attributes.position.array,neutral.eyes[i]);
  assert.equal(h.engine.snapshot().character.faceExpression, 'warm');
  h.pose({faceExpression: 'delighted', smileCurve: 1, cheekGlow: 1});
  assert.equal(h.engine.snapshot().character.faceExpression, 'delighted');
  assert.equal(h.engine.snapshot().character.digitalEyes, 2);
});

test('curiosity makes a readable raised-brow asymmetry while reassurance relaxes it', t => {
  const h = harness(t); h.pose({faceExpression: 'curious', faceBrowLift: .38, faceBrowTilt: .75});
  const left = h.get('expression-brow-left'), right = h.get('expression-brow-right');
  assert.ok(right.position.y - left.position.y > .05);
  const curiousRotation = right.rotation.z;
  h.pose({faceExpression: 'reassuring', faceBrowLift: -.16, faceBrowTilt: 0, smileCurve: .32});
  assert.equal(left.position.y, right.position.y); assert.notEqual(right.rotation.z, curiousRotation);
  assert.equal(h.engine.snapshot().character.faceExpression, 'reassuring');
});

test('expression cue inputs are bounded and malformed cues cannot deform face geometry into NaN', t => {
  const h = harness(t); h.pose({faceBrowLift: Infinity, faceBrowTilt: NaN, eyeSmile: -99, cheekGlow: 99, smileCurve: NaN, faceSignal: -Infinity, lidClosure: 99});
  for (const name of ['expression-brow-left', 'expression-brow-right', 'expression-eye-left', 'expression-eye-right', 'expression-cheek-left', 'expression-smile-glyph']) {
    const part = h.get(name); for (const vector of [part.position, part.rotation, part.scale]) for (const key of ['x', 'y', 'z']) assert.ok(Number.isFinite(vector[key]), name + '.' + key);
  }
  assert.ok(h.get('expression-cheek-left').scale.x <= 1.1);
  for(const name of ['expression-eye-left','expression-eye-right'])assert.ok([...h.get(name).geometry.attributes.position.array].every(Number.isFinite));
});

test('soles remain exactly supported when speech and legacy bob/scale inputs vary', t => {
  const h = harness(t), platform = new THREE.Box3().setFromObject(h.get('grounding-platform')).max.y;
  assert.ok(Math.abs(platform + 1.52) < 1e-6);
  for (const fields of [
    {state: 'idle', bob: -.4, stanceScale: .94, bodyRoll: -.3, bodyYaw: -.3, lean: -.3},
    {state: 'speaking', level: 1, bob: .4, stanceScale: 1.06, bodyRoll: .3, bodyYaw: .3, lean: .3},
    {state: 'success', emotion: 'celebrate', bob: .14, stanceScale: 1.04}
  ]) {
    h.pose(fields);
    for (const side of [-1, 1]) {
      const lowerEdge = new THREE.Box3().setFromObject(h.get('grounded-sole-' + side)).min.y;
      assert.ok(Math.abs(lowerEdge - platform) < 1e-6, 'sole/platform contact');
    }
  }
  assert.equal(h.engine.snapshot().grounding.feetFixed, true);
  assert.equal(h.engine.snapshot().grounding.upperBodyPivot, 'waist');
});

test('upper-body motion is bounded while the support root stays perfectly stationary', t => {
  const h = harness(t); h.pose({bodyRoll: 5, bodyYaw: 5, lean: 5, bodyDepth: 5, bob: 5});
  const rig = h.get('supported-upper-body');
  assert.ok(Math.abs(rig.rotation.x) <= .025); assert.ok(Math.abs(rig.rotation.y) <= .032); assert.ok(Math.abs(rig.rotation.z) <= .015);
  assert.deepEqual(rig.parent.position.toArray(), [0, 0, 0]); assert.deepEqual(rig.parent.scale.toArray(), [1, 1, 1]);
  assert.equal(h.declarations.interactionProfile.loopingBodyMotion, false);
});

test('negative Three.js head pitch visibly raises the actual eye anchor instead of looking down', t => {
  const h = harness(t); h.pose({headPitch: 0, headYaw: 0, headRoll: 0, lean: 0, bodyRoll: 0, bodyYaw: 0});
  const neutral = h.engine.gazeAnchor(); h.pose({headPitch: -.12, headYaw: 0, headRoll: 0, lean: 0, bodyRoll: 0, bodyYaw: 0});
  const up = h.engine.gazeAnchor(); h.pose({headPitch: .12, headYaw: 0, headRoll: 0, lean: 0, bodyRoll: 0, bodyYaw: 0});
  const down = h.engine.gazeAnchor();
  assert.ok(up.y < neutral.y, 'screen y decreases when looking up'); assert.ok(down.y > neutral.y, 'screen y increases when looking down');
});

test('face-anchor projection stays finite and accurate across tall and narrow layouts', t => {
  const h = harness(t);
  for (const [width, height] of [[480, 440], [180, 440], [390, 280], [980, 440]]) {
    h.resize(width, height); h.pose({headPitch: 0, headYaw: 0});
    const anchor = h.engine.gazeAnchor(), projected = h.get('articulated-expression-head').localToWorld(new THREE.Vector3(0, .112, .71)).project(h.camera);
    assert.ok(anchor.x > 0 && anchor.x < 1); assert.ok(anchor.y > 0 && anchor.y < 1);
    assert.ok(Math.abs(anchor.x - (projected.x + 1) / 2) < 1e-9); assert.ok(Math.abs(anchor.y - (1 - projected.y) / 2) < 1e-9);
    assert.deepEqual({...h.engine.snapshot().gazeAnchor}, {...anchor});
  }
});

test('transparent guide retains a bounded support cue and restores the studio on exit', t => {
  const h = harness(t); h.engine.setFloating(true);
  assert.equal(h.get('grounding-platform').visible, false); assert.equal(h.get('continuous-studio-sweep').visible, false);
  assert.equal(h.get('bounded-ground-contact-cue').visible, true); assert.equal(h.scene.background, null);
  const cue = h.get('bounded-ground-contact-cue'); assert.ok(cue.material.opacity <= .2); assert.ok(cue.geometry.parameters.radius < .7);
  h.engine.setFloating(false); assert.equal(h.get('grounding-platform').visible, true); assert.equal(cue.visible, false); assert.ok(h.scene.background.isCubeTexture);
});

test('time alone no longer spins a visible face ornament or loops a speech arm', t => {
  const h = harness(t); h.pose({state: 'speaking', speechEnergy: 0, mouthOpen: .01, armLiftLeft: 0, armLiftRight: 0, offer: 0, helloWave: 0});
  const before = h.get('supported-upper-body').children.filter(value => value.type === 'Group' && value.name !== 'articulated-expression-head').map(value => value.rotation.toArray());
  h.advance(30000); h.pose({state: 'speaking', speechEnergy: 0, mouthOpen: .01, armLiftLeft: 0, armLiftRight: 0, offer: 0, helloWave: 0});
  const after = h.get('supported-upper-body').children.filter(value => value.type === 'Group' && value.name !== 'articulated-expression-head').map(value => value.rotation.toArray());
  assert.deepEqual(after, before); assert.equal(h.get('retired-aperture-ornament').visible, false);
});

test('idle, silent speech and held output levels keep scene orientation and status shapes steady', t => {
  const h = harness(t), names = ['supported-upper-body', 'articulated-expression-head', 'retired-aperture-ornament', 'expression-speech-bar-0', 'expression-speech-bar-1', 'expression-speech-bar-2', 'expression-speech-bar-3', 'expression-speech-bar-4'];
  const capture = () => names.map(name => {const part = h.get(name); return {name, position: part.position.toArray(), rotation: part.rotation.toArray(), scale: part.scale.toArray()};});
  for (const state of ['idle', 'speaking', 'thinking', 'listening']) for (const speechEnergy of [0, .8]) {
    const fields = {state, speechEnergy, mouthOpen: speechEnergy, statusWave: 0, headPitch: 0, headYaw: 0, headRoll: 0, bodyRoll: 0, bodyYaw: 0, lean: 0, ringRotation: -.1, armLiftLeft: 0, armLiftRight: 0, offer: 0, helloWave: 0};
    h.advance(1000); h.pose(fields); const first = capture();
    h.advance(11000); h.pose(fields); assert.deepEqual(capture(), first, state + ' held energy ' + speechEnergy);
  }
  h.pose({state: 'speaking', speechEnergy: 0, statusWave: 0}); const silent = h.get('expression-speech-bar-2').scale.y;
  h.pose({state: 'speaking', speechEnergy: .8, statusWave: 0}); assert.ok(h.get('expression-speech-bar-2').scale.y > silent);
});

test('2-D fallback removes float, automatic look, orbit and speech gesture loops', () => {
  assert.doesNotMatch(css, /britesRobot(Float|Look|Orbit|Calm|SpeechGesture)/);
  const loopingAnimations = css.match(/animation:[^;}]*infinite/g) || [];
  assert.equal(loopingAnimations.length, 1); assert.match(loopingAnimations[0], /britesRobotBlink/);
  assert.match(css, /\.brites-avatar__robot\{animation:none;transform:none\}/);
  assert.match(css, /\.brites-avatar\[data-motion=reduced\] \*\{animation:none!important/);
});

test('fallback head cues retain gaze translations, so an acknowledgement never erases upward tracking', () => {
  for (const declaration of css.match(/transform:[^;}]*var\(--brites-talk-head\)[^;}]*[;}]/g) || []) {
    assert.match(declaration, /translate\(var\(--brites-head-gaze-x\),var\(--brites-head-gaze-y\)\)/);
  }
  assert.match(css, /\.brites-avatar__brow--left,\.brites-avatar__brow--right/);
  assert.match(css, /\.brites-avatar__smile-signal/);
});
