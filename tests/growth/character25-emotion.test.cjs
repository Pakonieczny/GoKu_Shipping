'use strict';
// CPU rig and synthetic lifecycle checks, not rendered-heart/native-audio proof.
const test = require('node:test'), assert = require('node:assert/strict');
const fs = require('node:fs'), vm = require('node:vm'), THREE = require('three');
const {JSDOM} = require('jsdom');
const avatar = require('../../brites-concierge-avatar.js');
async function fixture(t, reduced = false) {
  const dom = new JSDOM('<div id="mount"></div>', {url: 'https://sandbox.example/', pretendToBeVisual: true});
  const win = dom.window, jobs = new Map(); let clock = 1000, id = 0, hidden = false;
  Object.defineProperty(win.performance, 'now', {value: () => clock});
  Object.defineProperty(win.document, 'hidden', {get: () => hidden});
  win.setTimeout = (callback, delay) => {jobs.set(++id, {callback, due: clock + delay}); return id;};
  win.clearTimeout = key => jobs.delete(key);
  win.matchMedia = () => ({matches: reduced, addEventListener() {}, removeEventListener() {}});
  const guide = avatar.create({container: win.document.getElementById('mount'), visible: true, greetingOnOpen: false, loadScene: async () => {throw Error('Synthetic WebGL unavailable');}});
  await guide.ready; t.after(() => {guide.destroy(); win.close();});
  return {guide, jobs, advance(ms) {clock += ms; for (const [key, job] of [...jobs]) if (job.due <= clock) {jobs.delete(key); job.callback();}}, hide(value) {hidden = value; win.document.dispatchEvent(new win.Event('visibilitychange'));}};
}
test('appreciation smoothly enters, briefly holds, and disappears without a perpetual heart', () => {
  assert.equal(avatar.validEmotion('appreciated'), 'appreciated');
  const sample = elapsed => avatar.poseFor({emotion: 'appreciated', appreciationElapsed: elapsed});
  assert.equal(sample(0).heart, 0); assert.ok(sample(.08).heart > 0 && sample(.08).heart < 1);
  assert.equal(sample(.4).heart, 1); assert.ok(sample(1.3).heart < 1); assert.equal(sample(1.45).heart, 0);
  assert.equal(sample(.4).eyeColor, '#ed93aa'); assert.equal(sample(1.45).eyeColor, avatar.poseFor().eyeColor);
  assert.equal(sample(.4).offer, 0); assert.equal(sample(.4).armReach, 0);
});
test('brief appreciation restores the previous quiet emotion and removes SVG heart', async t => {
  const f = await fixture(t); f.guide.setEmotion('calm'); f.guide.setEmotion('appreciated');
  assert.equal(f.guide.element.dataset.heart, 'true'); assert.equal(f.guide.snapshot().mannerism.name, 'reassure');
  assert.ok(f.guide.element.querySelector('path.brites-avatar__heart'));
  f.advance(1500); assert.equal(f.guide.snapshot().emotion, 'calm'); assert.equal(f.guide.element.dataset.heart, 'false'); assert.equal(f.jobs.size, 0);
});
test('obsolete appreciation callback cannot override a newer frustration expression', async t => {
  const f = await fixture(t); f.guide.setEmotion('appreciated');
  const stale = [...f.jobs.values()].find(job => job.due === 2450).callback;
  f.guide.setEmotion('reassuring'); stale(); assert.equal(f.guide.snapshot().emotion, 'reassuring'); assert.equal(f.guide.element.dataset.heart, 'false');
});
test('pause and hide cancel appreciation and delayed callbacks never replay it', async t => {
  const f = await fixture(t);
  for (const [stop, resume] of [[() => f.guide.setPaused(true), () => f.guide.setPaused(false)], [() => f.hide(true), () => f.hide(false)]]) {
    f.guide.setEmotion('warm'); f.guide.setEmotion('appreciated'); stop();
    assert.equal(f.guide.snapshot().emotion, 'warm'); assert.equal(f.guide.element.dataset.heart, 'false'); assert.equal(f.jobs.size, 0);
    f.guide.setEmotion('appreciated'); assert.equal(f.guide.snapshot().emotion, 'warm'); resume(); f.advance(2000); assert.equal(f.guide.snapshot().emotion, 'warm');
  }
});
test('reduced motion has static brief appreciation without hand or mesh oscillation', async t => {
  const p = avatar.poseFor({emotion: 'appreciated', appreciationElapsed: .2, reducedMotion: true});
  const later = avatar.poseFor({emotion: 'appreciated', appreciationElapsed: 1, time: 20, reducedMotion: true});
  assert.deepEqual(later, p); assert.equal(p.heart, 1); assert.equal(p.bob, 0); assert.equal(p.phraseGesture, 0);
  const f = await fixture(t, true); f.guide.setEmotion('appreciated'); assert.equal(f.guide.snapshot().mannerism.active, false);
  f.advance(1500); assert.equal(f.guide.snapshot().emotion, null);
});
function meshFixture(t) {
  const source = fs.readFileSync(require.resolve('../../brites-concierge-avatar-scene.mjs'), 'utf8');
  const start = source.indexOf('  const head = new THREE.Group();'), end = source.indexOf('  const decoration = mesh(', start), beginPose = source.indexOf('  const whiteColor = new THREE.Color('), endPose = source.indexOf('  function render(pose', beginPose);
  const names = ['ivory', 'gold', 'paleGold', 'face', 'lidMaterial', 'eyeMaterial', 'pupilMaterial', 'glint', 'gemMaterial', 'corneaMaterial', 'irisMaterial', 'mouthMaterial'];
  const materials = Object.fromEntries(names.map(name => [name, new THREE.MeshPhysicalMaterial()]));
  const script = '(()=>{const geometries=new Set(),geometry=value=>{geometries.add(value);return value;},segments=(high,minimum=16)=>Math.max(minimum,Math.round(high*quality.geometryScale)),avatar=new THREE.Group();' + source.slice(start, end) + '\nconst key=new THREE.SpotLight(),eyeLight=new THREE.PointLight();let sampleTime=1,reducedMotion=false;' + source.slice(beginPose, endPose) + '\nreturn {apertureGeometry,halo,statusBars,pose:(value,time=1)=>{sampleTime=time;applyPose(value);},dispose:()=>geometries.forEach(value=>value.dispose())};})()';
  const f = vm.runInNewContext(script, {THREE, quality: avatar.qualityFor({width: 390}), ...materials, AVATAR_SCENE_DECLARATIONS: {stateColors: {idle: '#4aa8ff', speaking: '#ffcb79', listening: '#49c9ff', thinking: '#ab87ff'}}});
  t.after(() => {f.dispose(); Object.values(materials).forEach(value => value.dispose());}); return f;
}
test('production aperture morph has actual heart lobes/notch and finite geometry and normals', t => {
  const f = meshFixture(t); f.pose(avatar.poseFor({emotion: 'appreciated', appreciationElapsed: .4}));
  const points = f.apertureGeometry.attributes.position.array; let centreTop = -Infinity, lobeTop = -Infinity;
  for (let index = 0; index < points.length; index += 3) {assert.ok(Number.isFinite(points[index])); if (Math.abs(points[index]) < .025) centreTop = Math.max(centreTop, points[index + 1]); else if (Math.abs(points[index]) > .1) lobeTop = Math.max(lobeTop, points[index + 1]);}
  assert.ok(lobeTop > centreTop + .06, 'two lobes rise above central notch');
  assert.ok([...f.apertureGeometry.attributes.normal.array].every(Number.isFinite)); assert.equal(f.halo.visible, false);
  f.pose(avatar.poseFor()); assert.equal(f.halo.visible, true);
});
test('real aperture equalizer changes only with actual speech energy, keeping silence stable', t => {
  const f = meshFixture(t); const silent = avatar.poseFor({state: 'speaking', time: 1, level: 0});
  f.pose(silent, 1); const quiet = f.apertureGeometry.attributes.position.array.slice();
  f.pose(silent, 2); assert.deepEqual(f.apertureGeometry.attributes.position.array, quiet, 'no invented audio vibration');
  f.pose(avatar.poseFor({state: 'speaking', time: 1, level: .8}), 1); const audio = f.apertureGeometry.attributes.position.array.slice();
  f.pose(avatar.poseFor({state: 'speaking', time: 1, level: .8}), 2); assert.notDeepEqual(f.apertureGeometry.attributes.position.array, audio);
  assert.ok(f.statusBars.every(bar => bar.visible === false), 'speaking remains inside the eye, not a separate mouth');
});
