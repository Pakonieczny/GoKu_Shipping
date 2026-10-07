'use strict';
// Gesture scheduling/CPU poses only. No mock is evidence of a GPU render.
const test = require('node:test');
const assert = require('node:assert/strict');
const {JSDOM} = require('jsdom');
const avatar = require('../../brites-concierge-avatar.js');
function fixture(t, {visible = true, reduced = false, greetingOnOpen = true} = {}) {
  const dom = new JSDOM('<div id="mount"></div>', {url: 'https://sandbox.example/', pretendToBeVisual: true}), win = dom.window;
  let clock = 1000, hidden = false, intersect; const jobs = new Map(); let id = 0;
  Object.defineProperty(win.performance, 'now', {value: () => clock});
  Object.defineProperty(win.document, 'hidden', {get: () => hidden});
  win.setTimeout = (callback, delay) => {jobs.set(++id, {callback, due: clock + delay}); return id;};
  win.clearTimeout = value => jobs.delete(value);
  win.matchMedia = () => ({matches: reduced, addEventListener() {}, removeEventListener() {}});
  win.IntersectionObserver = class {constructor(callback) {intersect = callback;} observe() {} disconnect() {}};
  const api = avatar.create({container: win.document.getElementById('mount'), visible, greetingOnOpen, loadScene: async () => {throw Error('No WebGL');}});
  t.after(() => {api.destroy(); dom.window.close();});
  return {api, jobs, advance: ms => {clock += ms; for (const [key, job] of [...jobs]) if (job.due <= clock) {jobs.delete(key); job.callback();}}, hide: value => {hidden = value; win.document.dispatchEvent(new win.Event('visibilitychange'));}, intersect: value => intersect([{isIntersecting: value}])};
}
test('greeting is once per explicit opening and has a finite deadline', async t => {
  const f = fixture(t, {visible: false}); assert.equal(f.api.snapshot().mannerism.name, null);
  f.api.setVisible(true); const start = f.api.snapshot().mannerism;
  assert.equal(start.name, 'greet'); assert.equal(start.duration, 1.12); assert.equal(f.jobs.size, 1);
  f.api.setVisible(true); assert.equal(f.api.snapshot().mannerism.id, start.id);
  f.advance(1200); assert.equal(f.api.snapshot().mannerism.active, false); assert.equal(f.jobs.size, 0);
  f.api.setVisible(false); f.api.setVisible(true); assert.equal(f.api.snapshot().mannerism.id, start.id + 1);
});
test('semantic state transition interrupts previous gesture; repeated events never replay', t => {
  const f = fixture(t); f.api.setState('listening'); const first = f.api.snapshot().mannerism;
  assert.equal(first.name, 'acknowledge'); f.advance(200); f.api.setState('listening'); assert.equal(f.api.snapshot().mannerism.id, first.id);
  assert.ok(f.api.snapshot().mannerism.elapsed >= .199);
  f.api.setState('thinking'); assert.equal(f.api.snapshot().mannerism.name, 'focus'); assert.equal(f.jobs.size, 1);
  f.api.setState('success'); assert.equal(f.api.snapshot().mannerism.name, 'confirm'); f.advance(1300); assert.equal(f.api.snapshot().mannerism.name, null);
  f.api.setState('success'); assert.equal(f.jobs.size, 0);
});
test('pause, hidden document, offscreen and dismissal cancel gestures without late replay', t => {
  const f = fixture(t);
  for (const [stop, restore] of [[() => f.api.setPaused(true), () => f.api.setPaused(false)], [() => f.hide(true), () => f.hide(false)], [() => f.intersect(false), () => f.intersect(true)]]) {
    f.api.setState('idle'); f.api.setState('listening'); assert.equal(f.api.snapshot().mannerism.active, true); stop(); assert.equal(f.api.snapshot().mannerism.name, null); assert.equal(f.jobs.size, 0); restore(); assert.equal(f.api.snapshot().mannerism.name, null);
  }
  f.api.setState('success'); f.api.setVisible(false); f.advance(5000); assert.equal(f.api.snapshot().mannerism.name, null);
});
test('calm and reassuring convert confirmations to a quiet gesture and remove arm offers', t => {
  const f = fixture(t); f.api.setState('success'); assert.equal(f.api.snapshot().mannerism.name, 'confirm'); f.api.setEmotion('calm'); assert.equal(f.api.snapshot().mannerism.name, 'reassure');
  for (const emotion of ['calm', 'reassuring']) for (const name of Object.keys(avatar.MANNERISMS)) {
    const pose = avatar.poseFor({state: 'success', emotion, time: 0, elapsed: .6, mannerism: name, mannerismElapsed: .6});
    assert.equal(pose.bob, 0); assert.equal(pose.offer, 0); assert.ok(Math.abs(pose.nod) <= .15);
  }
});
test('eyes anticipate head and body and all mannerisms settle to neutral on deadline', () => {
  const early = avatar.mannerismFor({name: 'greet', elapsed: .09}); assert.ok(early.eye > 0); assert.equal(early.head, 0); assert.equal(early.body, 0);
  const head = avatar.mannerismFor({name: 'greet', elapsed: .18}); assert.ok(head.head > 0); assert.equal(head.body, 0);
  for (const [name, duration] of Object.entries(avatar.MANNERISMS)) {
    for (let elapsed = 0; elapsed <= duration; elapsed += .05) {
      const neutral = avatar.poseFor({time: elapsed, elapsed});
      const pose = avatar.poseFor({time: elapsed, elapsed, mannerism: name, mannerismElapsed: elapsed});
      for (const value of Object.values(pose)) if (typeof value === 'number') assert.ok(Number.isFinite(value));
      assert.ok(pose.offer >= 0 && pose.offer <= 1);
      assert.ok(Math.abs(pose.lean - neutral.lean) <= .04, 'gesture lean stays bounded relative to the contemporaneous idle pose');
    }
    const settled = avatar.mannerismFor({name, elapsed: duration}); assert.equal(settled.active, false); assert.equal(settled.offer, 0); assert.equal(settled.lift, 0);
    assert.deepEqual(avatar.poseFor({time: duration, elapsed: duration, mannerism: name, mannerismElapsed: duration}), avatar.poseFor({time: duration, elapsed: duration}), 'expired gesture returns to the current neutral pose, preserving ordinary idle motion');
  }
});
test('reduced motion keeps static state cues and schedules no animation timer', t => {
  const f = fixture(t, {reduced: true}); f.api.setState('success'); f.api.setVisible(false); f.api.setVisible(true); assert.equal(f.jobs.size, 0); assert.equal(f.api.snapshot().mannerism.active, false);
  const start = avatar.poseFor({state: 'listening', mannerism: 'greet', mannerismElapsed: .3, reducedMotion: true}), end = avatar.poseFor({state: 'listening', mannerism: 'greet', mannerismElapsed: 50, reducedMotion: true}); assert.deepEqual(start, end); assert.equal(start.offer, 0);
});
test('audio energy affects speaking emission without becoming a semantic confirmation', t => {
  const f = fixture(t); f.api.setState('speaking'); f.advance(1500); const serial = f.api.snapshot().mannerism.id;
  for (const level of [0, .2, .8, 1]) f.api.setLevel(level); assert.equal(f.api.snapshot().mannerism.name, null); assert.equal(f.api.snapshot().mannerism.id, serial); assert.equal(f.api.snapshot().state, 'speaking');
});
test('destroy clears deadline callback and prevents all future gestures', t => {
  const f = fixture(t); assert.equal(f.jobs.size, 1); f.api.destroy(); assert.equal(f.jobs.size, 0); f.api.setVisible(true); f.api.setState('success'); assert.equal(f.jobs.size, 0); assert.equal(f.api.snapshot().mannerism.active, false);
});

test('restored session suppresses greeting until explicit trigger, once per opening', t => {
  const f = fixture(t, {greetingOnOpen: false}); assert.equal(f.api.snapshot().mannerism.name, null);
  assert.equal(f.api.triggerGreeting(), true); const serial = f.api.snapshot().mannerism.id; assert.equal(f.api.triggerGreeting(), false); assert.equal(f.api.snapshot().mannerism.id, serial);
  f.api.setVisible(false); f.api.setVisible(true); assert.equal(f.api.snapshot().mannerism.name, null);
  assert.equal(f.api.triggerGreeting(), true); assert.equal(f.api.snapshot().mannerism.id, serial + 1);
});
function cpuMeshFixture() {
  const fs = require('node:fs'), vm = require('node:vm'), THREE = require('three'), source = fs.readFileSync(require.resolve('../../brites-concierge-avatar-scene.mjs'), 'utf8');
  const start = source.indexOf('  const head = new THREE.Group();'), end = source.indexOf('  const decoration = mesh(', start), poseStart = source.indexOf('  const whiteColor = new THREE.Color('), poseEnd = source.indexOf('  function render(pose', poseStart);
  assert.ok(start > 0 && end > start && poseStart > end && poseEnd > poseStart);
  const names = ['ivory','gold','paleGold','face','lidMaterial','eyeMaterial','pupilMaterial','glint','gemMaterial','corneaMaterial','irisMaterial','mouthMaterial'];
  const materials = Object.fromEntries(names.map(name => [name, ['irisMaterial', 'mouthMaterial', 'glint'].includes(name) ? new THREE.MeshBasicMaterial({toneMapped: false}) : new THREE.MeshPhysicalMaterial()]));
  const materialRegistry = new Set(Object.values(materials)), basic = options => {const value = new THREE.MeshBasicMaterial(options); materialRegistry.add(value); return value;};
  const script = '(()=>{const geometries=new Set(),geometry=value=>{geometries.add(value);return value;},segments=(high,minimum=16)=>Math.max(minimum,Math.round(high*quality.geometryScale)),avatar=new THREE.Group();' + source.slice(start,end) + '\nconst key=new THREE.SpotLight(),eyeLight=new THREE.PointLight();let sampleTime=1,reducedMotion=false;' + source.slice(poseStart,poseEnd) + '\nreturn {avatar,arms,eyes,statusBars,heartGlyphs,pose:value=>applyPose(value),destroy:()=>geometries.forEach(value=>value.dispose())};})()';
  const model = vm.runInNewContext(script, {THREE, quality: avatar.qualityFor({width:390}), basic, ...materials, AVATAR_SCENE_DECLARATIONS: {stateColors:{idle:'#4aa8ff',listening:'#49c9ff',thinking:'#ab87ff',speaking:'#70d8f1',success:'#72ddd1',error:'#ffc28e'}}});
  return {...model, dispose(){model.destroy(); materialRegistry.forEach(value => value.dispose());}};
}
test('production mesh does not celebrate a calm memorial confirmation', () => {
  const f = cpuMeshFixture(); try {
    f.pose(avatar.poseFor({state:'idle',emotion:'calm',time:0})); const neutral = f.eyes.map(eye => eye.apertureGeometry.attributes.position.array.slice());
    f.pose(avatar.poseFor({state:'success',emotion:'calm',time:0,elapsed:.6})); assert.equal(f.eyes.length, 2);
    f.eyes.forEach((eye, index) => assert.deepEqual(eye.apertureGeometry.attributes.position.array, neutral[index]));
    assert.ok(f.statusBars.every(({mesh}) => mesh.visible===false)); assert.ok(f.heartGlyphs.every(heart => !heart.visible));
    assert.equal(f.avatar.getObjectByName('expression-smile-glyph').visible,true);assert.equal(f.avatar.getObjectByName('expression-speech-mouth').visible,false);
    assert.equal(f.avatar.position.y, 0); for (const {group} of f.arms) assert.ok(group.rotation.y === 0);
  } finally {f.dispose();}
});
test('production mesh consumes eye deformation and only offers one arm during a greeting', () => {
  const f = cpuMeshFixture(); try {
    const base = avatar.poseFor({state:'idle',time:0}); f.pose(base); const before = f.eyes.map(eye => eye.apertureGeometry.attributes.position.array.slice());
    f.pose({...base,eyeScaleX:1.1,eyeScaleY:.85,eyeDeformation:.2});
    f.eyes.forEach((eye, index) => {assert.notDeepEqual(eye.apertureGeometry.attributes.position.array, before[index]); assert.ok([...eye.apertureGeometry.attributes.position.array].every(Number.isFinite)); assert.ok([...eye.apertureGeometry.attributes.normal.array].every(Number.isFinite));});
    f.pose(avatar.poseFor({state:'idle',time:0,mannerism:'greet',mannerismElapsed:.55}));
    const left = f.arms.find(row => row.side === -1).group, right = f.arms.find(row => row.side === 1).group;
    assert.ok(left.rotation.y === 0); assert.ok(right.rotation.y > 0); assert.ok(Math.abs(right.rotation.z) > Math.abs(left.rotation.z));
  } finally {f.dispose();}
});
